package dk.ku.di.dms.vms.sdk.embed.query;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.runnable.StoppableRunnable;
import dk.ku.di.dms.vms.modb.common.schema.network.query.QueryRequestEvent;
import dk.ku.di.dms.vms.modb.common.schema.network.query.QueryResultEvent;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static java.lang.System.Logger.Level.*;

public final class VmsQueryWorker extends StoppableRunnable {

    private static final System.Logger LOGGER = System.getLogger(VmsQueryWorker.class.getName());

    private final Semaphore writeLock = new Semaphore(1);
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

        // Initialize pool with fresh buffers (don't call retrieveByteBuffer here to avoid logic loops)
        this.writeBufferPool.add(MemoryManager.getTemporaryDirectBuffer(this.bufferSize));
        this.writeBufferPool.add(MemoryManager.getTemporaryDirectBuffer(this.bufferSize));
    }

    private void acquireLock() {
        try {
             LOGGER.log(DEBUG, "Acquiring Write Lock...");
            writeLock.acquire();
             LOGGER.log(DEBUG, "Write Lock Acquired.");
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while waiting for write lock", e);
        }
    }

    private void releaseLock() {
        writeLock.release();
         LOGGER.log(DEBUG, "Write Lock Released.");
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
                if (writeBuffer.remaining() < this.index.getRecordSize()) {
                     LOGGER.log(DEBUG, "Buffer full (Rows: " + count + "). Sending batch...");
                    this.sendBuffer(writeBuffer);

                    writeBuffer = this.retrieveByteBuffer();
                    QueryResultEvent.initBatch(writeBuffer, queryPayload.queryId());
                }

                this.index.copyRecordToBuffer(recordAddress, writeBuffer);
                count++;
            }

            LOGGER.log(INFO, "Iterator finished. Total rows scanned: " + count);

            if (writeBuffer.position() > QueryResultEvent.HEADER_SIZE) {
                LOGGER.log(INFO, "Sending final partial batch...");
                this.sendBuffer(writeBuffer);
            } else {
                this.returnByteBuffer(writeBuffer);
            }

            LOGGER.log(INFO, "Sending EndOfStream...");
            this.sendEndOfStream();

        } catch (Exception e) {
            LOGGER.log(ERROR, "CRITICAL ERROR in VmsQueryWorker", e);
        } finally {
            // Optional: Close channel here if not managed elsewhere
            // try { channel.close(); } catch (IOException e) { }
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
        LOGGER.log(INFO, "Query Vms Worker Exiting. QueryID: " + queryPayload.queryId());
    }

    private void sendBuffer(ByteBuffer buffer) {
        QueryResultEvent.finalizeBatch(buffer);
        buffer.flip();

        this.acquireLock();
        this.channel.write(buffer, timeout, TimeUnit.MILLISECONDS, buffer, this.batchWriteCompletionHandler);
    }

    private void sendEndOfStream() {
        ByteBuffer buffer = this.retrieveByteBuffer();
        QueryResultEvent.writeEndOfStream(buffer, queryPayload.queryId());
        buffer.flip();

        this.acquireLock();
        this.channel.write(buffer, timeout, TimeUnit.MILLISECONDS, buffer, this.batchWriteCompletionHandler);
    }

    private ByteBuffer retrieveByteBuffer() {
        ByteBuffer bb = this.writeBufferPool.poll();
        if(bb != null) {
            bb.clear();
            return bb;
        }
        LOGGER.log(WARNING, "Buffer pool empty, allocating new direct buffer.");
        return MemoryManager.getTemporaryDirectBuffer(this.bufferSize);
    }

    private void returnByteBuffer(ByteBuffer bb) {
        this.writeBufferPool.add(bb);
    }

    private final class BatchWriteCompletionHandler implements CompletionHandler<Integer, ByteBuffer> {
        @Override
        public void completed(Integer result, ByteBuffer byteBuffer) {
             LOGGER.log(DEBUG, "Network Write Completed. Bytes: " + result);

            if (byteBuffer.hasRemaining()) {
                LOGGER.log(WARNING, "Partial write detected! Resubmitting remaining bytes...");
                channel.write(byteBuffer, timeout, TimeUnit.MILLISECONDS, byteBuffer, this);
            } else {
                releaseLock();
                returnByteBuffer(byteBuffer);
            }
        }

        @Override
        public void failed(Throwable exc, ByteBuffer byteBuffer) {
            LOGGER.log(ERROR, "Network Write FAILED.", exc);
            releaseLock();
            returnByteBuffer(byteBuffer);
            stop();
        }
    }
}