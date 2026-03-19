package dk.ku.di.dms.vms.sdk.embed.query;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.runnable.StoppableRunnable;
import dk.ku.di.dms.vms.modb.common.schema.network.query.QueryRequestEvent;
import dk.ku.di.dms.vms.modb.common.schema.network.query.QueryResultEvent;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

import static java.lang.System.Logger.Level.ERROR;

public final class VmsQueryWorker extends StoppableRunnable {

    private static final System.Logger LOGGER = System.getLogger(VmsQueryWorker.class.getName());

    // A4 FIX: maximum number of spin iterations before we give up waiting
    // for the write lock. At ~1ns per spin this is roughly 50ms of tolerance.
    // Exceeding this almost certainly means the gateway dropped the connection.
    private static final int MAX_SPIN_RETRIES = 50_000;

    // A4: type 102 signals "worker aborted" to VmsResultIterator so it can
    // throw immediately instead of blocking on the next read forever.
    static final byte WORKER_ABORT_TYPE = 102;

    private static final VarHandle WRITE_SYNCHRONIZER;

    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            WRITE_SYNCHRONIZER = l.findVarHandle(VmsQueryWorker.class, "writeSynchronizer", int.class);
        } catch (Exception e) {
            throw new InternalError(e);
        }
    }

    @SuppressWarnings("unused")
    private volatile int writeSynchronizer;
    private final int bufferSize;
    private final int timeout;
    private final AsynchronousSocketChannel channel;
    private final Deque<ByteBuffer> writeBufferPool;
    private final BatchWriteCompletionHandler batchWriteCompletionHandler = new BatchWriteCompletionHandler();
    private final UniqueHashBufferIndex index;
    private final QueryRequestEvent.QueryPayload queryPayload;
    private final Iterator<Long> recordAddressIterator;
    private final Iterator<byte[]> byteRecordIterator;

    // A4 FIX: volatile error flag so BatchWriteCompletionHandler.failed()
    // can signal the run() loop to abort cleanly.
    private volatile boolean writeError = false;

    public VmsQueryWorker(AsynchronousSocketChannel channel, UniqueHashBufferIndex index,
                          Iterator<Long> recordAddressIterator,
                          QueryRequestEvent.QueryPayload queryPayload, int bufferSize, int timeout) {
        this.channel = channel;
        this.index = index;
        this.recordAddressIterator = recordAddressIterator;
        this.byteRecordIterator = null;
        this.queryPayload = queryPayload;
        this.bufferSize = bufferSize;
        this.timeout = timeout;
        this.writeBufferPool = new ConcurrentLinkedDeque<>();
        this.writeBufferPool.add(MemoryManager.getTemporaryDirectBuffer(this.bufferSize));
        this.writeBufferPool.add(MemoryManager.getTemporaryDirectBuffer(this.bufferSize));
    }

    public VmsQueryWorker(AsynchronousSocketChannel channel, Iterator<byte[]> byteRecordIterator,
                          QueryRequestEvent.QueryPayload queryPayload, int bufferSize, int timeout) {
        this.channel = channel;
        this.index = null;
        this.recordAddressIterator = null;
        this.byteRecordIterator = byteRecordIterator;
        this.queryPayload = queryPayload;
        this.bufferSize = bufferSize;
        this.timeout = timeout;
        this.writeBufferPool = new ConcurrentLinkedDeque<>();
        this.writeBufferPool.add(MemoryManager.getTemporaryDirectBuffer(this.bufferSize));
        this.writeBufferPool.add(MemoryManager.getTemporaryDirectBuffer(this.bufferSize));
    }

    public boolean tryAcquireLock() {
        return WRITE_SYNCHRONIZER.compareAndSet(this, 0, 1);
    }

    public void releaseLock() {
        WRITE_SYNCHRONIZER.setVolatile(this, 0);
    }

    @Override
    public void run() {
        long start = System.nanoTime();
        System.out.println("Starting Worker for QueryID: " + queryPayload.queryId()
                + " Table: " + queryPayload.tableName());

        ByteBuffer writeBuffer = this.retrieveByteBuffer();
        QueryResultEvent.initBatch(writeBuffer, queryPayload.queryId());
        long count = 0;

        try {
            if (this.recordAddressIterator != null) {
                while (this.recordAddressIterator.hasNext() && !writeError) {
                    Long recordAddress = this.recordAddressIterator.next();

                    if (writeBuffer.remaining() < 1024) {
                        this.sendBuffer(writeBuffer);
                        if (writeError) break;
                        writeBuffer = this.retrieveByteBuffer();
                        QueryResultEvent.initBatch(writeBuffer, queryPayload.queryId());
                    }

                    int lengthPos = writeBuffer.position();
                    writeBuffer.putInt(0);
                    int dataStart = writeBuffer.position();
                    this.index.copyRecordToBuffer(recordAddress, writeBuffer);
                    int dataEnd = writeBuffer.position();
                    writeBuffer.putInt(lengthPos, dataEnd - dataStart);

                    count++;
                    if (count % 5000 == 0)
                        System.out.println(">>> [VMS WORKER] Scanned " + count + " rows from " + queryPayload.tableName());
                }
            } else if (this.byteRecordIterator != null) {
                while (this.byteRecordIterator.hasNext() && !writeError) {
                    byte[] joinedData = this.byteRecordIterator.next();

                    if (writeBuffer.remaining() < joinedData.length + 4) {
                        this.sendBuffer(writeBuffer);
                        if (writeError) break;
                        writeBuffer = this.retrieveByteBuffer();
                        QueryResultEvent.initBatch(writeBuffer, queryPayload.queryId());
                    }

                    writeBuffer.putInt(joinedData.length);
                    writeBuffer.put(joinedData);

                    count++;
                    if (count % 5000 == 0)
                        System.out.println(">>> [VMS WORKER] Joined and sent " + count + " rows from " + queryPayload.tableName());
                }
            }

            System.out.println(">>> [VMS WORKER] " + queryPayload.tableName()
                    + " scan loop finished. Total rows: " + count);

            if (writeError) {
                // A4: write already failed, nothing left to send
                LOGGER.log(ERROR, ">>> [VMS WORKER] Aborting after write error. Rows sent: " + count);
                return;
            }

            if (writeBuffer.position() > QueryResultEvent.HEADER_SIZE) {
                this.sendBuffer(writeBuffer);
            } else {
                this.returnByteBuffer(writeBuffer);
            }

            System.out.println(">>> [VMS WORKER] Sending EndOfStream...");
            this.sendEndOfStream();

        } catch (Exception e) {
            LOGGER.log(ERROR, "CRITICAL ERROR in VmsQueryWorker: " + e.getMessage(), e);
        }

        long end = System.nanoTime();
        double durationMs = (end - start) / 1_000_000.0;
        System.out.printf(">>> [VMS WORKER] Finished. Mode: %d | Total Rows: %d | Time: %.2f ms%n",
                queryPayload.mode(), count, durationMs);
    }

    private void sendBuffer(ByteBuffer buffer) {
        QueryResultEvent.finalizeBatch(buffer);
        buffer.flip();

        // A4 FIX: bounded spin with retry limit.
        //
        // OLD: the CAS loop retried forever. If AsynchronousSocketChannel.write()
        // silently failed (e.g. gateway dropped the connection), the
        // BatchWriteCompletionHandler.failed() called stop() but the writeSynchronizer
        // was never released and the CAS spun indefinitely, burning a thread.
        //
        // NEW: after MAX_SPIN_RETRIES iterations we set writeError=true, send a
        // type-102 abort frame, and return. VmsResultIterator detects type 102
        // and throws instead of blocking forever on the next read.
        int retries = 0;
        while (!this.tryAcquireLock()) {
            if (writeError) return;
            try {
                if (retries < 10) {
                    Thread.onSpinWait();
                } else {
                    Thread.sleep(1);
                }
                retries++;
                if (retries >= MAX_SPIN_RETRIES) {
                    LOGGER.log(ERROR,
                            ">>> [VMS WORKER] Write lock spin limit reached. Gateway likely dead. Aborting.");
                    writeError = true;
                    sendAbortFrame();
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        this.channel.write(buffer, timeout, TimeUnit.MILLISECONDS, buffer, this.batchWriteCompletionHandler);
    }

    private void sendEndOfStream() {
        ByteBuffer buffer = this.retrieveByteBuffer();
        QueryResultEvent.writeEndOfStream(buffer, queryPayload.queryId());
        buffer.flip();

        int retries = 0;
        while (!this.tryAcquireLock()) {
            if (writeError) return;
            try {
                if (retries < 10) {
                    Thread.onSpinWait();
                } else {
                    Thread.sleep(1);
                }
                retries++;
                if (retries >= MAX_SPIN_RETRIES) {
                    LOGGER.log(ERROR,
                            ">>> [VMS WORKER] Write lock spin limit reached (EndOfStream). Aborting.");
                    writeError = true;
                    sendAbortFrame();
                    return;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        this.channel.write(buffer, timeout, TimeUnit.MILLISECONDS, buffer, this.batchWriteCompletionHandler);
    }

    // A4: sends a single-byte abort frame (type 102) to the gateway so
    // VmsResultIterator stops blocking and throws an IOException.
    private void sendAbortFrame() {
        try {
            ByteBuffer abortBuf = this.retrieveByteBuffer();
            abortBuf.put(WORKER_ABORT_TYPE);
            abortBuf.flip();
            // fire-and-forget — we don't care about completion at this point
            this.channel.write(abortBuf, timeout, TimeUnit.MILLISECONDS, abortBuf,
                    new CompletionHandler<Integer, ByteBuffer>() {
                        @Override public void completed(Integer r, ByteBuffer b) { returnByteBuffer(b); }
                        @Override public void failed(Throwable e, ByteBuffer b)  { returnByteBuffer(b); }
                    });
        } catch (Exception ignored) {}
    }

    private ByteBuffer retrieveByteBuffer() {
        ByteBuffer bb = this.writeBufferPool.poll();
        if (bb != null) { bb.clear(); return bb; }
        return MemoryManager.getTemporaryDirectBuffer(this.bufferSize);
    }

    private void returnByteBuffer(ByteBuffer bb) {
        this.writeBufferPool.add(bb);
    }

    private final class BatchWriteCompletionHandler implements CompletionHandler<Integer, ByteBuffer> {
        @Override
        public void completed(Integer result, ByteBuffer byteBuffer) {
            if (byteBuffer.hasRemaining()) {
                channel.write(byteBuffer, timeout, TimeUnit.MILLISECONDS, byteBuffer, this);
            } else {
                releaseLock();
                returnByteBuffer(byteBuffer);
            }
        }

        @Override
        public void failed(Throwable exc, ByteBuffer byteBuffer) {
            // A4 FIX: set writeError so the run() loop exits cleanly on next check.
            // OLD: just called stop() which set isRunning=false but the spin loop
            // kept spinning because writeSynchronizer was never released.
            LOGGER.log(ERROR, ">>> [VMS WORKER WRITE] Write failed: " + exc.getMessage());
            writeError = true;
            releaseLock();
            returnByteBuffer(byteBuffer);
            stop();
        }
    }
}