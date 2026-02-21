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

import static java.lang.System.Logger.Level.*;

public final class VmsQueryWorker extends StoppableRunnable {

    private static final System.Logger LOGGER = System.getLogger(VmsQueryWorker.class.getName());
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

    public VmsQueryWorker(
            AsynchronousSocketChannel channel,
            UniqueHashBufferIndex index,
            Iterator<Long> recordAddressIterator,
            QueryRequestEvent.QueryPayload queryPayload,
            int bufferSize,
            int timeout
    ) {
        this.channel = channel;
        this.index = index;
        this.recordAddressIterator = recordAddressIterator;
        this.queryPayload = queryPayload;
        this.bufferSize = bufferSize;
        this.timeout = timeout;
        this.writeBufferPool = new ConcurrentLinkedDeque<>();
        this.writeBufferPool.add(MemoryManager.getTemporaryDirectBuffer(this.bufferSize));
        this.writeBufferPool.add(MemoryManager.getTemporaryDirectBuffer(this.bufferSize));
    }

    @SuppressWarnings("StatementWithEmptyBody")
    public void acquireLock(){
        while(! WRITE_SYNCHRONIZER.compareAndSet(this, 0, 1) );
    }

    public boolean tryAcquireLock(){
        return WRITE_SYNCHRONIZER.compareAndSet(this, 0, 1);
    }

    public void releaseLock(){
        WRITE_SYNCHRONIZER.setVolatile(this, 0);
    }

    @Override
    public void run() {
        long start = System.nanoTime();
        LOGGER.log(INFO, "Starting Scan for QueryID: " + queryPayload.queryId() + " Table: " + queryPayload.tableName());

        ByteBuffer writeBuffer = this.retrieveByteBuffer();
        QueryResultEvent.initBatch(writeBuffer, queryPayload.queryId());

        long count = 0;

        try {
            while (this.recordAddressIterator.hasNext()) {
                Long recordAddress = this.recordAddressIterator.next();

                if (writeBuffer.remaining() < 1024) {
                    this.sendBuffer(writeBuffer);
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
            }

            LOGGER.log(INFO, "Iterator finished. Total rows scanned: " + count);

            if (writeBuffer.position() > QueryResultEvent.HEADER_SIZE) {
                this.sendBuffer(writeBuffer);
            } else {
                this.returnByteBuffer(writeBuffer);
            }

            LOGGER.log(INFO, "Sending EndOfStream...");
            this.sendEndOfStream();

        } catch (Exception e) {
            LOGGER.log(ERROR, "CRITICAL ERROR in VmsQueryWorker", e);
        }

        long end = System.nanoTime();

        double durationMs = (end - start) / 1_000_000.0;
        double throughput = 0;
        if (durationMs > 0) {
            throughput = (count / durationMs) * 1000;
        }

        LOGGER.log(INFO, String.format(
                "[Metrics-VMS] QueryID: %d | Rows: %d | Time: %.2f ms | Throughput: %.0f rows/sec",
                queryPayload.queryId(), count, durationMs, throughput
        ));
    }

    private void sendBuffer(ByteBuffer buffer) {
        QueryResultEvent.finalizeBatch(buffer);
        buffer.flip();

        int retries = 0;
        while (!this.tryAcquireLock()) {
            try {
                if (retries < 10) {
                    Thread.onSpinWait();
                } else {
                    Thread.sleep(1);
                }
                retries++;
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
            try {
                if (retries < 10) { Thread.onSpinWait(); } else { Thread.sleep(1); }
                retries++;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }

        this.channel.write(buffer, timeout, TimeUnit.MILLISECONDS, buffer, this.batchWriteCompletionHandler);
    }

    private ByteBuffer retrieveByteBuffer() {
        ByteBuffer bb = this.writeBufferPool.poll();
        if(bb != null) { bb.clear(); return bb; }
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
            releaseLock();
            returnByteBuffer(byteBuffer);
            stop();
        }
    }
}