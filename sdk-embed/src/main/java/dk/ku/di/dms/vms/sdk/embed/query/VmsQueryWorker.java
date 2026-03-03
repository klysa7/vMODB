package dk.ku.di.dms.vms.sdk.embed.query;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.runnable.StoppableRunnable;
import dk.ku.di.dms.vms.modb.common.schema.network.query.QueryRequestEvent;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class VmsQueryWorker extends StoppableRunnable {

    private final int bufferSize;
    private final int timeout;
    private final AsynchronousSocketChannel channel;
    private final UniqueHashBufferIndex index;
    private final QueryRequestEvent.QueryPayload queryPayload;

    private final Iterator<Long> recordAddressIterator;
    private final Iterator<byte[]> byteRecordIterator;

    public VmsQueryWorker(AsynchronousSocketChannel channel, UniqueHashBufferIndex index, Iterator<Long> recordAddressIterator, QueryRequestEvent.QueryPayload queryPayload, int bufferSize, int timeout) {
        this.channel = channel;
        this.index = index;
        this.recordAddressIterator = recordAddressIterator;
        this.byteRecordIterator = null;
        this.queryPayload = queryPayload;
        this.bufferSize = Math.max(bufferSize, 8192);
        this.timeout = timeout <= 0 ? 5000 : timeout;
    }

    public VmsQueryWorker(AsynchronousSocketChannel channel, Iterator<byte[]> byteRecordIterator, QueryRequestEvent.QueryPayload queryPayload, int bufferSize, int timeout) {
        this.channel = channel;
        this.index = null;
        this.recordAddressIterator = null;
        this.byteRecordIterator = byteRecordIterator;
        this.queryPayload = queryPayload;
        this.bufferSize = Math.max(bufferSize, 8192);
        this.timeout = timeout <= 0 ? 5000 : timeout;
    }

    @Override
    public void run() {
        long start = System.nanoTime();
        System.out.println(">>> [VMS-WORKER] Starting Raw C-Struct Byte Streamer for QueryID: " + queryPayload.queryId());

        ByteBuffer writeBuffer = MemoryManager.getTemporaryDirectBuffer(this.bufferSize);
        writeBuffer.clear();

        // -------------------------------------------------------------------------------------
        // THE FIX: Manually write the CUSTOM HEADER that the Order VMS expects!
        // The header is exactly 16 bytes: [Byte 99] [Int Size] [Long QueryID] [Int RowCount]
        // -------------------------------------------------------------------------------------
        writeBuffer.put((byte) 99); // QUERY_RESULT_TYPE
        int sizePos = writeBuffer.position();
        writeBuffer.putInt(0); // Placeholder for total payload size
        writeBuffer.putLong(queryPayload.queryId());
        int countPos = writeBuffer.position();
        writeBuffer.putInt(0); // Placeholder for row count

        long totalCount = 0;
        int batchRowCount = 0;

        try {
            if (this.recordAddressIterator != null) {
                // =========================================================
                // WAREHOUSE VMS: Block-Copy C-Structs to TCP Buffer
                // =========================================================
                while (this.recordAddressIterator.hasNext() && this.isRunning()) {
                    Long recordAddress = this.recordAddressIterator.next();

                    // If the buffer doesn't have enough space for the next struct, flush it!
                    if (writeBuffer.remaining() < 2048) {
                        // Backfill the header placeholders before sending
                        int payloadSize = writeBuffer.position() - sizePos - 4;
                        writeBuffer.putInt(sizePos, payloadSize);
                        writeBuffer.putInt(countPos, batchRowCount);

                        System.out.println(">>> [VMS-WORKER] Flushing TCP batch of " + batchRowCount + " structs (Size: " + payloadSize + " bytes).");
                        this.sendBuffer(writeBuffer);

                        // Reset the buffer and write a new custom header for the next batch
                        writeBuffer.clear();
                        writeBuffer.put((byte) 99);
                        sizePos = writeBuffer.position();
                        writeBuffer.putInt(0);
                        writeBuffer.putLong(queryPayload.queryId());
                        countPos = writeBuffer.position();
                        writeBuffer.putInt(0);
                        batchRowCount = 0;
                    }

                    // Write the struct length as a 4-byte int, then copy the raw struct bytes
                    int lengthPos = writeBuffer.position();
                    writeBuffer.putInt(0); // length placeholder
                    int dataStart = writeBuffer.position();

                    // Directly copy off-heap C-struct (skipping header) into NIO!
                    this.index.copyRecordToBuffer(recordAddress, writeBuffer);

                    int dataEnd = writeBuffer.position();
                    writeBuffer.putInt(lengthPos, dataEnd - dataStart); // backfill struct length

                    batchRowCount++;
                    totalCount++;
                }
            } else if (this.byteRecordIterator != null) {
                // =========================================================
                // ORDER VMS: Write concatenated byte arrays to Gateway
                // =========================================================
                while (this.byteRecordIterator.hasNext() && this.isRunning()) {
                    byte[] joinedData = this.byteRecordIterator.next();

                    if (writeBuffer.remaining() < joinedData.length + 4) {
                        int payloadSize = writeBuffer.position() - sizePos - 4;
                        writeBuffer.putInt(sizePos, payloadSize);
                        writeBuffer.putInt(countPos, batchRowCount);

                        this.sendBuffer(writeBuffer);

                        writeBuffer.clear();
                        writeBuffer.put((byte) 99);
                        sizePos = writeBuffer.position();
                        writeBuffer.putInt(0);
                        writeBuffer.putLong(queryPayload.queryId());
                        countPos = writeBuffer.position();
                        writeBuffer.putInt(0);
                        batchRowCount = 0;
                    }

                    writeBuffer.putInt(joinedData.length);
                    writeBuffer.put(joinedData);

                    batchRowCount++;
                    totalCount++;
                }
            }

            System.out.println(">>> [VMS-WORKER] Table scan finished. Total rows encoded: " + totalCount);

            // Flush the final batch if there is any data left
            if (batchRowCount > 0) {
                int payloadSize = writeBuffer.position() - sizePos - 4;
                writeBuffer.putInt(sizePos, payloadSize);
                writeBuffer.putInt(countPos, batchRowCount);
                System.out.println(">>> [VMS-WORKER] Flushing final TCP batch of " + batchRowCount + " rows.");
                this.sendBuffer(writeBuffer);
            }

            System.out.println(">>> [VMS-WORKER] Sending EndOfStream flag...");
            writeBuffer.clear();
            writeBuffer.put((byte) 100); // END_OF_STREAM_TYPE
            writeBuffer.putInt(8);       // EOF payload size
            writeBuffer.putLong(queryPayload.queryId());
            this.sendBuffer(writeBuffer);

        } catch (Exception e) {
            System.out.println(">>> [VMS-WORKER] CRITICAL ERROR: " + e.getMessage());
            e.printStackTrace();
        } finally {
            MemoryManager.releaseTemporaryDirectBuffer(writeBuffer);
        }

        double durationMs = (System.nanoTime() - start) / 1_000_000.0;
        System.out.println(String.format(">>> [VMS-WORKER] Stream Closed. Yielded %d Rows. Latency: %.2f ms", totalCount, durationMs));
    }

    private void sendBuffer(ByteBuffer buffer) throws Exception {
        if (buffer.position() == 0) return;

        buffer.flip();

        while (buffer.hasRemaining() && this.isRunning()) {
            CountDownLatch writeLatch = new CountDownLatch(1);
            AtomicBoolean writeFailed = new AtomicBoolean(false);

            channel.write(buffer, timeout, TimeUnit.MILLISECONDS, buffer, new CompletionHandler<Integer, ByteBuffer>() {
                @Override
                public void completed(Integer result, ByteBuffer attachment) {
                    if (result == -1) writeFailed.set(true);
                    writeLatch.countDown();
                }

                @Override
                public void failed(Throwable exc, ByteBuffer attachment) {
                    System.out.println(">>> [VMS-WORKER-TCP] FAILED TO WRITE! " + exc.getMessage());
                    writeFailed.set(true);
                    writeLatch.countDown();
                }
            });

            boolean completedInTime = writeLatch.await(timeout + 2000, TimeUnit.MILLISECONDS);

            if (!completedInTime || writeFailed.get()) {
                this.stop();
                throw new java.io.IOException("TCP Socket dropped before stream finished.");
            }
        }
    }
}