package dk.ku.di.dms.vms.coordinator.query;

import dk.ku.di.dms.vms.modb.common.schema.network.query.QueryRequestEvent;
import dk.ku.di.dms.vms.modb.common.schema.network.query.QueryResultEvent;
import dk.ku.di.dms.vms.modb.common.schema.network.query.ScanRule;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.runnable.StoppableRunnable;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

import static java.lang.System.Logger.Level.DEBUG;
import static java.lang.System.Logger.Level.ERROR;

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

    //what to bring from VMS
    private final ScanRule scanRule;

    public VmsQueryWorker(
            AsynchronousSocketChannel channel,
            UniqueHashBufferIndex index,
            ScanRule scanRule,
            int bufferSize,
            int timeout
    ) {
        this.channel = channel;
        this.index = index;
        this.scanRule = scanRule;
        this.bufferSize = bufferSize;
        this.timeout = timeout;
        this.writeBufferPool = new ConcurrentLinkedDeque<>();
        this.writeBufferPool.addFirst(retrieveByteBuffer());
        this.writeBufferPool.addFirst(retrieveByteBuffer());
    }


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
        LOGGER.log(System.Logger.Level.INFO, "Starting Scan for QueryID: " + scanRule.queryId());

        //start opening, iterating the index storage
        var iterator = this.index.iterator();

        ByteBuffer writeBuffer = this.retrieveByteBuffer();

        //reserve some bytes to identify that its a query Result
        //maybe which query i am and how big i am
        QueryResultEvent.initBatch(writeBuffer, scanRule.queryId());

        try {//keep copying as we have data
            while (iterator.hasNext()) {
                //lets say we have memory address, take it from the storaege copy the bytes
                long recordAddress = iterator.address();


                //maybe a way to see before we write
                if (writeBuffer.remaining() < this.index.getRecordSize()) {
                    this.sendBuffer(writeBuffer);
                    writeBuffer = this.retrieveByteBuffer();
                    QueryResultEvent.initBatch(writeBuffer, scanRule.queryId());
                }

                //copy raw bytes to the buffer that ll be moved to the calcite
                this.index.copyRecordToBuffer(recordAddress, writeBuffer);
            }

            if (writeBuffer.position() > QueryResultEvent.HEADER_SIZE) {
                this.sendBuffer(writeBuffer);
            }

            this.sendEndOfStream();

        } catch (Exception e) {
            LOGGER.log(System.Logger.Level.ERROR, "Error during query scan", e);
        }

        LOGGER.log(System.Logger.Level.INFO, "Query Scan Finished.");
    }


    private void sendBuffer(ByteBuffer buffer) {
        //Finalize the batch write total size at the start
        QueryResultEvent.finalizeBatch(buffer);

        buffer.flip();

        // Lock -> Async Write -> Callback releases lock
        this.acquireLock();
        this.channel.write(buffer, timeout, TimeUnit.MILLISECONDS, buffer, this.batchWriteCompletionHandler);
    }

    //Send a small message saying "Done"
    private void sendEndOfStream() {
        ByteBuffer buffer = this.retrieveByteBuffer();
        QueryResultEvent.writeEndOfStream(buffer, scanRule.queryId());
        buffer.flip();
        this.acquireLock();
        this.channel.write(buffer, timeout, TimeUnit.MILLISECONDS, buffer, this.batchWriteCompletionHandler);
    }

    private ByteBuffer retrieveByteBuffer() {
        ByteBuffer bb = this.writeBufferPool.poll();
        if(bb != null) return bb;
        return MemoryManager.getTemporaryDirectBuffer(this.bufferSize);
    }

    private void returnByteBuffer(ByteBuffer bb) {
        bb.clear();
        this.writeBufferPool.add(bb);
    }


    private final class BatchWriteCompletionHandler implements CompletionHandler<Integer, ByteBuffer> {
        @Override
        public void completed(Integer result, ByteBuffer byteBuffer) {
            LOGGER.log(DEBUG, "Leader: Message with size " + result + " has been sent to: ");
            if(byteBuffer.hasRemaining()) {
                // keep the lock and send the remaining
                channel.write(byteBuffer, timeout, TimeUnit.MILLISECONDS, byteBuffer, this);
            } else {
                releaseLock();
//                if(options.logging()){
//                    loggingWriteBuffers.add(byteBuffer);
//                } else {
//                    returnByteBuffer(byteBuffer);
//                }
            }
        }

        @Override
        public void failed(Throwable exc, ByteBuffer byteBuffer) {
            releaseLock();
            LOGGER.log(ERROR, "Leader: ERROR on writing batch of events to ");
            returnByteBuffer(byteBuffer);
            //stop failed scan?
            stop();
        }
    }

//    @Override
//    public void stop() {
//        super.stop();
//        this.channel.close();
//    }
}