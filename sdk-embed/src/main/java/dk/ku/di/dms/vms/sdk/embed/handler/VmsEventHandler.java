package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum;
import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.*;
import dk.ku.di.dms.vms.modb.common.schema.network.control.ConsumerSet;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.query.QueryRequestEvent;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.definition.Table;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import dk.ku.di.dms.vms.modb.query.execution.raw.GeneralRowView;
import dk.ku.di.dms.vms.modb.query.execution.raw.RawPredicate;
import dk.ku.di.dms.vms.modb.transaction.TransactionManager;
import dk.ku.di.dms.vms.sdk.core.metadata.VmsRuntimeMetadata;
import dk.ku.di.dms.vms.sdk.core.operational.InboundEvent;
import dk.ku.di.dms.vms.sdk.core.operational.OutboundEventResult;
import dk.ku.di.dms.vms.sdk.core.scheduler.IVmsTransactionResult;
import dk.ku.di.dms.vms.sdk.embed.channel.VmsEmbedInternalChannels;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplicationOptions;
import dk.ku.di.dms.vms.sdk.embed.query.VmsQueryWorker;
import dk.ku.di.dms.vms.web_common.HttpUtils;
import dk.ku.di.dms.vms.web_common.IHttpHandler;
import dk.ku.di.dms.vms.web_common.ModbHttpServer;
import dk.ku.di.dms.vms.web_common.NetworkUtils;
import dk.ku.di.dms.vms.web_common.channel.JdkAsyncChannel;
import dk.ku.di.dms.vms.web_common.meta.ConnectionMetadata;

import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

import static dk.ku.di.dms.vms.modb.common.schema.network.Constants.*;
import static dk.ku.di.dms.vms.modb.common.schema.network.query.QueryResultEvent.END_OF_STREAM_TYPE;
import static dk.ku.di.dms.vms.modb.common.schema.network.query.QueryResultEvent.QUERY_RESULT_TYPE;
import static java.lang.System.Logger.Level.*;

public final class VmsEventHandler extends ModbHttpServer {

    private static final System.Logger LOGGER = System.getLogger(VmsEventHandler.class.getName());

    public static class JoinContext {
        public final QueryRequestEvent.QueryPayload payload;
        public final AsynchronousSocketChannel gatewayChannel;
        public final int[] localJoinColumnIndices;
        public final int[] remoteJoinColumnIndices;
        public final Schema remoteSchema;

        // FIX 1: Updated map type to match Object[] pipeline
        public final Map<Integer, byte[]> broadcastBuffer = new ConcurrentHashMap<>();

        public JoinContext(QueryRequestEvent.QueryPayload payload,
                           AsynchronousSocketChannel gatewayChannel,
                           int[] localJoinColumnIndices,
                           int[] remoteJoinColumnIndices,
                           Schema remoteSchema) {
            this.payload = payload;
            this.gatewayChannel = gatewayChannel;
            this.localJoinColumnIndices = localJoinColumnIndices;
            this.remoteJoinColumnIndices = remoteJoinColumnIndices;
            this.remoteSchema = remoteSchema;
        }
    }

    public record ColRefDTO(int columnPosition) {}
    public record PredicateDTO(String columnName, String expression, Object value) {}

    private final Map<Long, JoinContext> activeJoins = new ConcurrentHashMap<>();

    private final AsynchronousServerSocketChannel serverSocket;

    private final AsynchronousChannelGroup group;

    private final VmsEmbedInternalChannels vmsInternalChannels;

    private final VmsNode me;

    private final VmsRuntimeMetadata vmsMetadata;

    private final Map<String, List<IVmsContainer>> eventToConsumersMap;

    private final Map<IdentifiableNode, IVmsContainer> consumerVmsContainerMap;

    private final Map<Integer, ConnectionMetadata> producerConnectionMetadataMap;

    private final ITransactionManager transactionManager;

    private final IVmsSerdesProxy serdesProxy;

    private final VmsHandlerOptions options;

    private final IHttpHandler httpHandler;

    private ServerNode leader;

    private ConnectionMetadata leaderConnectionMetadata;

    private LeaderWorker leaderWorker;

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private final Set<String> queuesLeaderSubscribesTo;

    private final Map<Long, BatchContext> batchContextMap;

    public final Map<Long, BatchMetadata> trackingBatchMap;

    public static final class BatchMetadata {
        public int numberTIDsExecuted;
        public long maxTidExecuted;
    }

    private final Map<Long, Map<String, Long>> tidToPrecedenceMap;

    public static VmsEventHandler build(VmsNode me,
                                        ITransactionManager transactionalHandler,
                                        VmsEmbedInternalChannels vmsInternalChannels,
                                        VmsRuntimeMetadata vmsMetadata,
                                        VmsApplicationOptions options,
                                        IHttpHandler httpHandler,
                                        IVmsSerdesProxy serdesProxy){
        try {
            return new VmsEventHandler(me, vmsMetadata,
                    transactionalHandler, vmsInternalChannels,
                    new VmsEventHandler.VmsHandlerOptions( options.maxSleep(), options.networkBufferSize(),
                            options.soBufferSize(), options.networkThreadPoolSize(), options.networkSendTimeout(),
                            options.numVmsWorkers(), options.isLogging(), options.isCheckpointing()),
                    httpHandler, serdesProxy);
        } catch (IOException e){
            throw new RuntimeException("Error on setting up event handler: "+e.getCause()+ " "+ e.getMessage());
        }
    }

    public record VmsHandlerOptions(int maxSleep,
                                    int networkBufferSize,
                                    int soBufferSize,
                                    int networkThreadPoolSize,
                                    int networkSendTimeout,
                                    int numVmsWorkers,
                                    boolean logging,
                                    boolean checkpointing) {}

    private VmsEventHandler(VmsNode me,
                            VmsRuntimeMetadata vmsMetadata,
                            ITransactionManager transactionManager,
                            VmsEmbedInternalChannels vmsInternalChannels,
                            VmsHandlerOptions options,
                            IHttpHandler httpHandler,
                            IVmsSerdesProxy serdesProxy) throws IOException {
        super();

        if(options.networkThreadPoolSize > 0){
            this.group = AsynchronousChannelGroup.withFixedThreadPool(
                    options.networkThreadPoolSize,
                    Thread.ofPlatform().name("vms-network-thread").factory()
            );
            this.serverSocket = AsynchronousServerSocketChannel.open(this.group);
        } else {
            this.group = null;
            this.serverSocket = AsynchronousServerSocketChannel.open(null);
        }
        this.serverSocket.bind(me.asInetSocketAddress());

        this.vmsInternalChannels = vmsInternalChannels;
        this.me = me;

        this.vmsMetadata = vmsMetadata;
        this.eventToConsumersMap = new HashMap<>();
        this.consumerVmsContainerMap = new HashMap<>();
        this.producerConnectionMetadataMap = new ConcurrentHashMap<>();

        this.serdesProxy = serdesProxy;

        this.batchContextMap = new ConcurrentHashMap<>();
        this.trackingBatchMap = new ConcurrentHashMap<>();
        this.tidToPrecedenceMap = new ConcurrentHashMap<>();

        this.transactionManager = transactionManager;

        this.leader = new ServerNode("0.0.0.0",0);
        this.leader.off();

        this.queuesLeaderSubscribesTo = new HashSet<>();

        this.options = options;
        this.httpHandler = httpHandler;
    }

    @Override
    public void run() {
        this.serverSocket.accept(null, new AcceptCompletionHandler());
        LOGGER.log(DEBUG,this.me.identifier+": Accept handler setup");
    }

    public void processOutputEvent(IVmsTransactionResult txResult) {
        LOGGER.log(DEBUG,this.me.identifier+": New transaction result in event handler. TID = "+ txResult.tid());
        if (txResult.getOutboundEventResult().outputQueue() != null) {
            Map<String, Long> precedenceMap = this.tidToPrecedenceMap.get(txResult.tid());
            if (precedenceMap != null) {
                precedenceMap.remove(this.me.identifier);
                String precedenceMapUpdated = this.serdesProxy.serializeMap(precedenceMap);
                this.processOutputEvent(txResult.getOutboundEventResult(), precedenceMapUpdated);
            } else {
                LOGGER.log(ERROR, this.me.identifier + ": No precedence map found for TID: " + txResult.tid());
            }
        }
        this.updateBatchStats(txResult.getOutboundEventResult());
    }

    private void updateBatchStats(OutboundEventResult outputEvent) {
        BatchMetadata batchMetadata = this.updateBatchMetadataAtomically(outputEvent);
        if(!this.batchContextMap.containsKey(outputEvent.batch())) return;
        BatchContext thisBatch = this.batchContextMap.get(outputEvent.batch());
        if(thisBatch.numberOfTIDsBatch != batchMetadata.numberTIDsExecuted) {
            return;
        }
        LOGGER.log(DEBUG, this.me.identifier + ": All TIDs for the batch " + thisBatch.batch + " have been executed");
        thisBatch.setStatus(BatchContext.BATCH_COMPLETED);
        if (thisBatch.terminal) {
            LOGGER.log(DEBUG, this.me.identifier + ": Requesting leader worker to send batch " + thisBatch.batch + " complete");
            this.leaderWorker.queueMessage(BatchComplete.of(thisBatch.batch, this.me.identifier));
        }
        if(this.options.checkpointing()){
            LOGGER.log(DEBUG, this.me.identifier + ": Requesting checkpoint for batch " + thisBatch.batch);
            thisBatch.setStatus(BatchContext.CHECKPOINTING);
            submitBackgroundTask(()->this.checkpoint(thisBatch.batch, batchMetadata.maxTidExecuted));
        } else {
            submitBackgroundTask(()->transactionManager.cleanup(batchMetadata.maxTidExecuted));
        }
        this.cleanUpBatchInfo(thisBatch.batch);
    }

    private void cleanUpBatchInfo(long batch) {
        this.batchContextMap.remove(batch);
        this.trackingBatchMap.remove(batch);
        this.tidToPrecedenceMap.remove(batch);
    }

    private BatchMetadata updateBatchMetadataAtomically(OutboundEventResult outputEvent) {
        return this.trackingBatchMap.compute(outputEvent.batch(),
                (ignored, y) -> {
                    BatchMetadata toMod = y;
                    if(toMod == null){
                        toMod = new BatchMetadata();
                    }
                    toMod.numberTIDsExecuted += 1;
                    if(toMod.maxTidExecuted < outputEvent.tid()){
                        toMod.maxTidExecuted = outputEvent.tid();
                    }
                    return toMod;
                });
    }

    private void connectToReceivedConsumerSet(Map<String, List<IdentifiableNode>> receivedConsumerVms) {
        Map<IdentifiableNode, List<String>> consumerToEventsMap = new HashMap<>();
        for(Map.Entry<String,List<IdentifiableNode>> entry : receivedConsumerVms.entrySet()) {
            for(IdentifiableNode consumer : entry.getValue()){
                consumerToEventsMap.computeIfAbsent(consumer, (ignored) -> new ArrayList<>()).add(entry.getKey());
            }
        }
        for( Map.Entry<IdentifiableNode,List<String>> consumerEntry : consumerToEventsMap.entrySet() ) {
            for(int i = 0; i < this.options.numVmsWorkers; i++){
                this.initConsumerVmsWorker(consumerEntry.getKey(), consumerEntry.getValue(), i);
            }
        }
    }

    private static final boolean INFORM_BATCH_ACK = false;

    private void checkpoint(long batch, long maxTid) {
        this.transactionManager.checkpoint(maxTid);
        if(INFORM_BATCH_ACK) {
            this.leaderWorker.queueMessage(BatchCommitAck.of(batch, this.me.identifier));
        }
    }

    private void processOutputEvent(OutboundEventResult outputEvent, String precedenceMap){
        Class<?> clazz = this.vmsMetadata.queueToEventMap().get(outputEvent.outputQueue());
        String objStr = this.serdesProxy.serialize(outputEvent.output(), clazz);
        List<IVmsContainer> consumerVMSs = this.eventToConsumersMap.get(outputEvent.outputQueue());
        if(consumerVMSs == null || consumerVMSs.isEmpty()){
            LOGGER.log(DEBUG,this.me.identifier+": An output event (queue: "+outputEvent.outputQueue()+") has no target virtual microservices.");
            return;
        }
        TransactionEvent.PayloadRaw payload = TransactionEvent.of(outputEvent.tid(), outputEvent.batch(), outputEvent.outputQueue(), objStr, precedenceMap);
        for(IVmsContainer consumerVmsContainer : consumerVMSs) {
            LOGGER.log(DEBUG,this.me.identifier+": An output event (queue: " + outputEvent.outputQueue() + ") will be queued to VMS: " + consumerVmsContainer.identifier());
            consumerVmsContainer.queue(payload);
        }
    }

    public void initConsumerVmsWorker(IdentifiableNode node, List<String> outputEvents, int identifier){
        if(this.producerConnectionMetadataMap.containsKey(node.hashCode())){
            LOGGER.log(WARNING,"The node "+ node.host+" "+ node.port+" already contains a connection as a producer");
        }
        if(this.me.hashCode() == node.hashCode()){
            LOGGER.log(ERROR,this.me.identifier+" is receiving itself as consumer: "+ node.identifier);
            return;
        }
        ConsumerVmsWorker consumerVmsWorker = ConsumerVmsWorker.build(this.me, node,
                () -> JdkAsyncChannel.create(this.group),
                this.options,
                this.serdesProxy);
        Thread.ofPlatform().name("vms-consumer-"+node.identifier+"-"+identifier)
                .inheritInheritableThreadLocals(false)
                .start(consumerVmsWorker);
        if(!this.consumerVmsContainerMap.containsKey(node)){
            if(this.options.numVmsWorkers == 1) {
                this.consumerVmsContainerMap.put(node, consumerVmsWorker);
            } else {
                MultiVmsContainer multiVmsContainer = new MultiVmsContainer(consumerVmsWorker, node, this.options.numVmsWorkers);
                this.consumerVmsContainerMap.put(node, multiVmsContainer);
            }
            for (String outputEvent : outputEvents) {
                LOGGER.log(INFO,me.identifier+ " adding "+outputEvent+" to consumers map with "+node.identifier);
                this.eventToConsumersMap.computeIfAbsent(outputEvent, (ignored) -> new ArrayList<>());
                this.eventToConsumersMap.get(outputEvent).add(consumerVmsWorker);
            }
        } else {
            IVmsContainer vmsContainer = this.consumerVmsContainerMap.get(node);
            if(vmsContainer instanceof MultiVmsContainer multiVmsContainer){
                multiVmsContainer.addConsumerVms(consumerVmsWorker);
            } else {
                vmsContainer.stop();
                this.consumerVmsContainerMap.put(node, consumerVmsWorker);
            }
        }
    }

    private final class VmsReadCompletionHandler implements CompletionHandler<Integer, Integer> {

        private final IdentifiableNode node;
        private final ConnectionMetadata connectionMetadata;
        private final ByteBuffer readBuffer;

        public VmsReadCompletionHandler(IdentifiableNode node,
                                        ConnectionMetadata connectionMetadata,
                                        ByteBuffer byteBuffer){
            this.node = node;
            this.connectionMetadata = connectionMetadata;
            this.readBuffer = byteBuffer;
            LIST_BUFFER.add(new ArrayList<>(1024));
        }

        @Override
        public void completed(Integer result, Integer startPos) {
            if(result == -1){
                LOGGER.log(WARNING,me.identifier+": VMS "+node.identifier+" has disconnected!");
                try {
                    this.connectionMetadata.channel.close();
                } catch (IOException ignored) { }
                return;
            }
            if(startPos == 0){
                this.readBuffer.flip();
            }
            byte messageType = this.readBuffer.get();
            switch (messageType) {
                case (BATCH_OF_EVENTS) -> {
                    int bufferSize = this.getBufferSize();
                    if(this.readBuffer.remaining() < bufferSize){
                        this.fetchMoreBytes(startPos);
                        return;
                    }
                    this.processBatchOfEvents(this.readBuffer);
                }
                case (EVENT) -> {
                    int bufferSize = this.getBufferSize();
                    if(this.readBuffer.remaining() < bufferSize){
                        this.fetchMoreBytes(startPos);
                        return;
                    }
                    this.processSingleEvent(this.readBuffer);
                }
                default -> {
                    LOGGER.log(ERROR,me.identifier+": Unknown message type "+messageType+" received from: "+node.identifier);
                    if(!isRunning()){
                        return;
                    }
                }
            }
            if(this.readBuffer.hasRemaining()){
                this.completed(result, this.readBuffer.position());
            } else {
                this.setUpNewRead();
            }
        }

        private int getBufferSize() {
            int bufferSize = Integer.MAX_VALUE;
            if(this.readBuffer.remaining() > Integer.BYTES) {
                bufferSize = this.readBuffer.getInt();
                bufferSize -= 1 + Integer.BYTES;
            }
            return bufferSize;
        }

        private void fetchMoreBytes(Integer startPos) {
            this.readBuffer.position(startPos);
            this.readBuffer.compact();
            this.connectionMetadata.channel.read(this.readBuffer, 0, this);
        }

        private void setUpNewRead() {
            this.readBuffer.clear();
            this.connectionMetadata.channel.read(this.readBuffer, 0, this);
        }

        private void processSingleEvent(ByteBuffer readBuffer) {
            try {
                TransactionEvent.Payload payload = TransactionEvent.read(readBuffer);
                LOGGER.log(DEBUG,me.identifier+": 1 event received from "+node.identifier+"\n"+payload);
                if (vmsMetadata.queueToEventMap().containsKey(payload.event())) {
                    InboundEvent inboundEvent = buildInboundEvent(payload);
                    vmsInternalChannels.transactionInputQueue().add(inboundEvent);
                }
            } catch (Exception e) {
                if(e instanceof BufferUnderflowException)
                    LOGGER.log(ERROR,me.identifier + ": Buffer underflow exception while reading event: " + e);
                else
                    LOGGER.log(ERROR,me.identifier + ": Unknown exception: " + e);
            }
        }

        private void processBatchOfEvents(ByteBuffer readBuffer) {
            List<InboundEvent> inboundEvents = LIST_BUFFER.poll();
            if(inboundEvents == null) inboundEvents = new ArrayList<>(1024*10);
            try {
                int count = readBuffer.getInt();
                LOGGER.log(DEBUG,me.identifier + ": Batch of [" + count + "] events received from " + node.identifier);
                TransactionEvent.Payload payload;
                int i = 0;
                while (i < count) {
                    payload = TransactionEvent.read(readBuffer);
                    LOGGER.log(DEBUG, me.identifier+": Processed TID "+payload.tid());
                    if (vmsMetadata.queueToEventMap().containsKey(payload.event())) {
                        InboundEvent inboundEvent = buildInboundEvent(payload);
                        inboundEvents.add(inboundEvent);
                    }
                    i++;
                }
                if(count != inboundEvents.size()){
                    LOGGER.log(WARNING,me.identifier + ": Batch of [" +count+ "] events != from "+inboundEvents.size()+" that will be pushed to worker " + node.identifier);
                }
                vmsInternalChannels.transactionInputQueue().addAll(inboundEvents);
                LOGGER.log(DEBUG, "Number of inputs pending processing: "+vmsInternalChannels.transactionInputQueue().size());
            } catch(Exception e){
                if (e instanceof BufferUnderflowException)
                    LOGGER.log(ERROR,me.identifier + ": Buffer underflow exception while reading batch: " + e);
                else
                    LOGGER.log(ERROR,me.identifier + ": Unknown exception: " + e);
            } finally {
                inboundEvents.clear();
                LIST_BUFFER.add(inboundEvents);
            }
        }

        @Override
        public void failed(Throwable exc, Integer carryOn) {
            LOGGER.log(ERROR,me.identifier+": Error on reading VMS message from "+node.identifier+"\n"+exc);
            exc.printStackTrace(System.out);
            this.setUpNewRead();
        }
    }

    private final class BroadcastReceiverHandler implements CompletionHandler<Integer, Integer> {
        private final AsynchronousSocketChannel channel;
        private ByteBuffer readBuffer;

        private boolean readingHeader = true;
        private byte currentType = -1;
        private int expectedPayloadSize = -1;
        private long currentQueryId = -1;

        public BroadcastReceiverHandler(AsynchronousSocketChannel channel, ByteBuffer readBuffer) {
            this.channel = channel;
            this.readBuffer = readBuffer;
            LOGGER.log(INFO, ">>> [BROADCAST-RX] Initialized Byte Receiver!");
        }

        @Override
        public void completed(Integer result, Integer attachment) {
            if (result == -1) return;

            // ALWAYS flip unconditionally after a read.
            readBuffer.flip();

            while (readBuffer.hasRemaining()) {
                if (readingHeader) {
                    if (readBuffer.remaining() < 1) break;

                    readBuffer.mark();
                    currentType = readBuffer.get();

                    if (currentType == END_OF_STREAM_TYPE) {
                        if (readBuffer.remaining() < 12) { readBuffer.reset(); break; }
                        readBuffer.getInt(); // Skip size
                        long queryId = readBuffer.getLong();

                        JoinContext ctx = activeJoins.remove(queryId);
                        if (ctx != null) {
                            LOGGER.log(INFO, ">>> [ORDER VMS] Broadcast END received. Hash Map size: " + ctx.broadcastBuffer.size());
                            LOGGER.log(INFO, ">>> [ORDER VMS] Triggering Hash Probe...");

                            TransactionManager tm = (TransactionManager) transactionManager;
                            Iterator<byte[]> joinIter = tm.getJoinIterator(ctx.payload.tableName(), ctx.broadcastBuffer, ctx.localJoinColumnIndices);
                            VmsQueryWorker worker = new VmsQueryWorker(ctx.gatewayChannel, joinIter, ctx.payload, options.networkBufferSize(), options.networkSendTimeout());
                            Thread.ofPlatform().name("query-worker-join-" + queryId).start(worker);
                        }
                        try { channel.close(); } catch (Exception ignored) {}
                        return;
                    }
                    else if (currentType == QUERY_RESULT_TYPE) {
                        // Header is exactly 16 bytes: [byte 99] [int size] [long queryId] [int rowCount]
                        if (readBuffer.remaining() < 16) { readBuffer.reset(); break; }
                        expectedPayloadSize = readBuffer.getInt();
                        currentQueryId = readBuffer.getLong();
                        readBuffer.getInt(); // Consume rowCount to align stream
                        readingHeader = false;
                    }
                    else {
                        LOGGER.log(ERROR, ">>> [ORDER VMS] Corrupted type byte: " + currentType + ". Dropping connection.");
                        try { channel.close(); } catch (Exception ignored) {}
                        return;
                    }
                }
                else {
                    // Staging Buffer: Ensure full payload is available!
                    if (readBuffer.remaining() < expectedPayloadSize) {
                        break;
                    }

                    int bytesRemaining = expectedPayloadSize;
                    JoinContext ctx = activeJoins.get(currentQueryId);

                    if (ctx != null) {
                        while (bytesRemaining > 0) {
                            int rowSize = readBuffer.getInt();
                            byte[] rowData = new byte[rowSize];
                            readBuffer.get(rowData);
                            bytesRemaining -= (4 + rowSize);

                            try {
                                GeneralRowView remoteRowView = new GeneralRowView(ctx.remoteSchema);
                                remoteRowView.setByteArray(rowData);
                                int joinHash = remoteRowView.getCompositeHash(ctx.remoteJoinColumnIndices);
                                ctx.broadcastBuffer.put(joinHash, rowData);
                            } catch (Exception e) {
                                LOGGER.log(ERROR, ">>> [ORDER VMS] Failed to hash RowView!", e);
                            }
                        }
                    } else {
                        // Skip the payload if the JoinContext is null
                        readBuffer.position(readBuffer.position() + bytesRemaining);
                    }

                    readingHeader = true;
                }
            }

            // ALWAYS compact unconditionally before the next read
            if (readBuffer.position() == 0 && readBuffer.limit() == readBuffer.capacity()) {
                ByteBuffer newBuffer = ByteBuffer.allocate(readBuffer.capacity() * 2);
                newBuffer.put(readBuffer);
                readBuffer = newBuffer;
            } else {
                readBuffer.compact();
            }

            // Ask for more bytes
            channel.read(readBuffer, 0, this);
        }

        @Override public void failed(Throwable exc, Integer attachment) { LOGGER.log(ERROR, "Broadcast failed", exc); }
    }

    private final class UnknownNodeReadCompletionHandler implements CompletionHandler<Integer, Void> {

        private final AsynchronousSocketChannel channel;
        private final ByteBuffer buffer;

        public UnknownNodeReadCompletionHandler(AsynchronousSocketChannel channel, ByteBuffer buffer) {
            this.channel = channel;
            this.buffer = buffer;
        }

        @Override
        public void completed(Integer result, Void void_) {
            String remoteAddress = "";
            try { remoteAddress = channel.getRemoteAddress().toString(); } catch (IOException ignored) { }
            if(result == -1 || result == 0){
                try { this.channel.close(); } catch (IOException ignored) {}
                return;
            }

            buffer.flip();
            if (buffer.remaining() < 1) {
                buffer.compact();
                channel.read(buffer, null, this);
                return;
            }

            buffer.mark();
            byte messageIdentifier = buffer.get();

            if (messageIdentifier == QUERY_RESULT_TYPE || messageIdentifier == QueryRequestEvent.QUERY_REQUEST_TYPE || messageIdentifier == END_OF_STREAM_TYPE) {
                LOGGER.log(INFO, ">>> [VMS HANDSHAKE] Direct Query detected! Bypassing Presentation.");

                // Rewind the buffer perfectly for the custom OLAP handlers
                buffer.reset();

                // Allocate a new buffer ONLY for OLAP handlers so they can safely resize it if needed
                ByteBuffer olapBuffer = ByteBuffer.allocate(buffer.capacity());
                olapBuffer.put(buffer);

                if (messageIdentifier == QueryRequestEvent.QUERY_REQUEST_TYPE) {
                    ConnectionMetadata dummyMeta = new ConnectionMetadata("gateway".hashCode(), ConnectionMetadata.NodeType.GATEWAY, this.channel);
                    IdentifiableNode dummyNode = new IdentifiableNode("gateway", "localhost", 8095);
                    GatewayReadCompletionHandler handler = new GatewayReadCompletionHandler(dummyNode, dummyMeta, olapBuffer);
                    handler.completed(result, 0);
                } else {
                    BroadcastReceiverHandler handler = new BroadcastReceiverHandler(this.channel, olapBuffer);
                    handler.completed(result, 0);
                }
                return;
            }

            // Restore the buffer completely to its original state for the legacy logic
            buffer.reset();

            if(messageIdentifier != PRESENTATION){
                String request = StandardCharsets.UTF_8.decode(this.buffer).toString();
                if(HttpUtils.isHttpClient(request)){
                    this.buffer.flip();
                    HttpReadCompletionHandler readCompletionHandler = new HttpReadCompletionHandler(
                            new ConnectionMetadata("http_client".hashCode(),
                                    ConnectionMetadata.NodeType.HTTP_CLIENT,
                                    this.channel),
                            this.buffer,
                            MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize),
                            httpHandler);
                    try { NetworkUtils.configure(this.channel, options.soBufferSize()); } catch (IOException ignored) { }
                    readCompletionHandler.parse(new HttpReadCompletionHandler.RequestTracking());
                } else {
                    LOGGER.log(WARNING, me.identifier + ": A node is trying to connect without a presentation message.\n"+request);
                    this.buffer.clear();
                    MemoryManager.releaseTemporaryDirectBuffer(this.buffer);
                    try { this.channel.close(); } catch (IOException ignored) { }
                }
                return;
            }

            // Consume the PRESENTATION byte again to align with legacy logic expectations
            this.buffer.get();

            if (this.buffer.remaining() < 1) {
                this.buffer.compact();
                this.channel.read(this.buffer, null, this);
                return;
            }

            byte nodeTypeIdentifier = this.buffer.get();

            // The legacy logic expects the position to be 2 (after reading PRESENTATION and Type)
            switch (nodeTypeIdentifier) {
                case (Presentation.SERVER_TYPE) -> this.processServerPresentation();
                case (Presentation.VMS_TYPE) -> this.processVmsPresentation();
                case (Presentation.GATEWAY_TYPE) -> this.processGatewayPresentation();
                default -> this.processUnknownNodeType(nodeTypeIdentifier);
            }
        }

        private void processGatewayPresentation() {
            LOGGER.log(INFO, me.identifier + ": Processing presentation message from Gateway");
            VmsNode gatewayNode = Presentation.readVms(this.buffer, serdesProxy);

            ConnectionMetadata connMetadata = new ConnectionMetadata(
                    gatewayNode.hashCode(), ConnectionMetadata.NodeType.GATEWAY, this.channel);

            GatewayReadCompletionHandler handler = new GatewayReadCompletionHandler(gatewayNode, connMetadata, this.buffer);

            if (this.buffer.hasRemaining()) {
                int leftoverBytes = this.buffer.remaining();
                this.buffer.compact();
                handler.completed(leftoverBytes, 0);
            } else {
                this.buffer.clear();
                this.channel.read(this.buffer, 0, handler);
            }
        }

        private final class GatewayReadCompletionHandler implements CompletionHandler<Integer, Integer> {
            private final IdentifiableNode gateway;
            private final ConnectionMetadata connectionMetadata;
            private ByteBuffer readBuffer;

            private boolean readingHeader = true;
            private int expectedPayloadSize = -1;

            public GatewayReadCompletionHandler(IdentifiableNode gateway, ConnectionMetadata connectionMetadata, ByteBuffer readBuffer) {
                this.gateway = gateway;
                this.connectionMetadata = connectionMetadata;
                this.readBuffer = readBuffer;
            }

            @Override
            public void completed(Integer result, Integer startPos) {
                if (result == -1) {
                    try { connectionMetadata.channel.close(); } catch (IOException ignored) {}
                    return;
                }

                // ALWAYS flip unconditionally
                readBuffer.flip();

                while (readBuffer.hasRemaining()) {
                    if (readingHeader) {
                        if (readBuffer.remaining() < 5) break;

                        readBuffer.mark();
                        byte type = readBuffer.get();

                        if (type == QueryRequestEvent.QUERY_REQUEST_TYPE) {
                            expectedPayloadSize = readBuffer.getInt();
                            readingHeader = false;
                        } else {
                            try { connectionMetadata.channel.close(); } catch (Exception ignored) {}
                            return;
                        }
                    } else {
                        // Staging Buffer - Wait for the entire JSON payload to arrive!
                        if (readBuffer.remaining() < expectedPayloadSize) {
                            break;
                        }

                        try {
                            processQueryRequest(readBuffer);
                        } catch (Exception e) {
                            LOGGER.log(ERROR, ">>> [GATEWAY-RX] FATAL ERROR parsing JSON!", e);
                        }

                        readingHeader = true;
                    }
                }

                // ALWAYS compact unconditionally
                if (readBuffer.position() == 0 && readBuffer.limit() == readBuffer.capacity()) {
                    ByteBuffer newBuffer = ByteBuffer.allocate(readBuffer.capacity() * 2);
                    newBuffer.put(readBuffer);
                    readBuffer = newBuffer;
                } else {
                    readBuffer.compact();
                }

                connectionMetadata.channel.read(readBuffer, 0, this);
            }

            private void processQueryRequest(java.nio.ByteBuffer buffer) {
                try {
                    var payload = QueryRequestEvent.read(buffer);
                    LOGGER.log(INFO, ">>> [VMS] Received Request | Table: " + payload.tableName() + " | Mode: " + payload.mode());

                    TransactionManager transactionManagerGateway = (TransactionManager) transactionManager;
                    Table localTable = transactionManagerGateway.catalog.get(payload.tableName());
                    Schema localSchema = localTable.schema();

                    List<RawPredicate> predicates = null;
                    if (payload.predicates() != null && payload.predicates().length > 0) {
                        String jsonString = new String(payload.predicates(), StandardCharsets.UTF_8);
                        PredicateDTO[] dtos = (PredicateDTO[]) serdesProxy.deserialize(jsonString, PredicateDTO[].class);

                        if (dtos != null) {
                            predicates = new ArrayList<>();
                            for (PredicateDTO dto : dtos) {
                                ExpressionTypeEnum expr = ExpressionTypeEnum.valueOf(dto.expression());
                                String targetColName = dto.columnName();

                                int modbColIdx = -1;
                                String[] schemaColNames = localSchema.columnNames();
                                for (int i = 0; i < schemaColNames.length; i++) {
                                    if (schemaColNames[i].equalsIgnoreCase(targetColName)) { modbColIdx = i; break; }
                                }

                                if (modbColIdx == -1) continue;

                                final int finalColIdx = modbColIdx;
                                Object val = dto.value();
                                DataType type = localSchema.columnDataType(finalColIdx);

                                predicates.add(new RawPredicate() {
                                    @Override
                                    public boolean matches(Object[] row) {
                                        Object memoryValue = row[finalColIdx];
                                        if (memoryValue == null) return false;

                                        int cmp = 0;
                                        switch (type) {
                                            case INT: cmp = Integer.compare(((Number) memoryValue).intValue(), ((Number) val).intValue()); break;
                                            case LONG: cmp = Long.compare(((Number) memoryValue).longValue(), ((Number) val).longValue()); break;
                                            case DOUBLE: cmp = Double.compare(((Number) memoryValue).doubleValue(), ((Number) val).doubleValue()); break;
                                        }

                                        switch (expr) {
                                            case EQUALS: return cmp == 0;
                                            case GREATER_THAN: return cmp > 0;
                                            case LESS_THAN: return cmp < 0;
                                            case GREATER_THAN_OR_EQUAL: return cmp >= 0;
                                            case LESS_THAN_OR_EQUAL: return cmp <= 0;
                                            case NOT_EQUALS: return cmp != 0;
                                            default: return false;
                                        }
                                    }
                                });
                            }
                        }
                    }

                    final List<RawPredicate> finalPredicates = predicates;

                    if (payload.mode() == QueryRequestEvent.MODE_SCAN_TO_GATEWAY) {
                        executeStandardScan(payload, finalPredicates, transactionManagerGateway, (AsynchronousSocketChannel) connectionMetadata.channel);
                    }
                    else if (payload.mode() == QueryRequestEvent.MODE_BROADCAST_TO_VMS) {
                        String targetAddress = new String(payload.routingData(), StandardCharsets.UTF_8);
                        String[] parts = targetAddress.split(":");
                        String host = parts[0];
                        int port = Integer.parseInt(parts[1]);

                        AsynchronousSocketChannel targetChannel = AsynchronousSocketChannel.open(group);
                        targetChannel.connect(new java.net.InetSocketAddress(host, port), null, new CompletionHandler<Void, Void>() {
                            @Override
                            public void completed(Void result, Void attachment) {
                                executeStandardScan(payload, finalPredicates, transactionManagerGateway, targetChannel);
                            }
                            @Override
                            public void failed(Throwable exc, Void attachment) {}
                        });
                    }
                    else if (payload.mode() == QueryRequestEvent.MODE_RECEIVE_AND_JOIN) {
                        String routingStr = new String(payload.routingData(), StandardCharsets.UTF_8);
                        String[] parts = routingStr.split("\\|");
                        String[] remoteParts = parts[0].split(":");

                        String remoteTableName = remoteParts[0];
                        String[] remoteColNames = remoteParts[1].split(",");
                        String[] localColNames = parts[1].split(",");

                        Table remoteTable = transactionManagerGateway.catalog.get(remoteTableName);
                        Schema remoteSchema;

                        if (remoteTable != null) {
                            remoteSchema = remoteTable.schema();
                        } else if (remoteTableName.equalsIgnoreCase("customer")) {
                            String[] colNames = {"c_id", "c_d_id", "c_w_id", "c_first", "c_middle", "c_last", "c_since", "c_credit", "c_credit_lim", "c_discount", "c_balance", "c_ytd_payment", "c_payment_cnt", "c_data"};
                            DataType[] colTypes = {DataType.INT, DataType.INT, DataType.INT, DataType.STRING, DataType.STRING, DataType.STRING, DataType.DATE, DataType.STRING, DataType.FLOAT, DataType.FLOAT, DataType.FLOAT, DataType.FLOAT, DataType.INT, DataType.STRING};
                            int[] pkCols = {0, 1, 2};
                            remoteSchema = new Schema(colNames, colTypes, pkCols, null, false);
                        } else { return; }

                        int[] remoteCols = new int[remoteColNames.length];
                        String[] rNames = remoteSchema.columnNames();
                        for (int i = 0; i < remoteColNames.length; i++) {
                            for (int j = 0; j < rNames.length; j++) {
                                if (rNames[j].equalsIgnoreCase(remoteColNames[i])) { remoteCols[i] = j; break; }
                            }
                        }

                        int[] localJoinCols = new int[localColNames.length];
                        String[] lNames = localSchema.columnNames();
                        for (int i = 0; i < localColNames.length; i++) {
                            for (int j = 0; j < lNames.length; j++) {
                                if (lNames[j].equalsIgnoreCase(localColNames[i])) { localJoinCols[i] = j; break; }
                            }
                        }

                        activeJoins.put(payload.queryId(), new JoinContext(
                                payload, (AsynchronousSocketChannel) connectionMetadata.channel,
                                localJoinCols, remoteCols, remoteSchema
                        ));
                    }
                } catch (Exception e) {
                    LOGGER.log(ERROR, "Error processing query request", e);
                }
            }

            private void executeStandardScan(QueryRequestEvent.QueryPayload payload, List<RawPredicate> predicates, TransactionManager tm, AsynchronousSocketChannel outputChannel) {
                UniqueHashBufferIndex index = (UniqueHashBufferIndex) tm.getIndex(payload.tableName());
                Iterator<Long> addressIterator = tm.getScanIterator(payload.tableName(), predicates);

                VmsQueryWorker worker = new VmsQueryWorker(outputChannel, index, addressIterator, payload, options.networkBufferSize(), options.networkSendTimeout());
                Thread.ofPlatform().name("query-worker-" + payload.queryId()).start(worker);
            }

            @Override
            public void failed(Throwable exc, Integer attachment) { }
        }


        private void processServerPresentation() {
            LOGGER.log(INFO,me.identifier+": Start processing presentation message from a node claiming to be a server");
            if(!leader.isActive()) {
                ConnectionFromLeaderProtocol connectionFromLeader = new ConnectionFromLeaderProtocol(this.channel, this.buffer);
                connectionFromLeader.processLeaderPresentation();
            } else {
                this.buffer.get();
                ServerNode serverNode = Presentation.readServer(this.buffer);
                if(serverNode.asInetSocketAddress().equals(leader.asInetSocketAddress())) {
                    LOGGER.log(INFO, me.identifier + ": Leader requested an additional connection");
                    this.buffer.clear();
                    channel.read(buffer, 0, new LeaderReadCompletionHandler(new ConnectionMetadata(leader.hashCode(), ConnectionMetadata.NodeType.SERVER, channel), buffer));
                } else {
                    try {
                        LOGGER.log(WARNING,"Dropping a connection attempt from a node claiming to be leader");
                        this.channel.close();
                    } catch (Exception ignored) {}
                }
            }
        }

        private void processVmsPresentation() {
            LOGGER.log(INFO,me.identifier+": Start processing presentation message from a node claiming to be a VMS");

            VmsNode producerVms = Presentation.readVms(this.buffer, serdesProxy);
            LOGGER.log(INFO, me.identifier+": Producer VMS received:\n"+producerVms);
            this.buffer.clear();

            ConnectionMetadata connMetadata = new ConnectionMetadata(
                    producerVms.hashCode(),
                    ConnectionMetadata.NodeType.VMS,
                    this.channel
            );

            if(consumerVmsContainerMap.containsKey(producerVms)){
                LOGGER.log(WARNING,me.identifier+": The node "+producerVms.host+" "+producerVms.port+" already contains a connection as a consumer");
            }

            if(producerConnectionMetadataMap.containsKey(producerVms.hashCode())) {
                LOGGER.log(INFO, me.identifier+": Setting up additional consumption from producer "+producerVms);
            } else {
                producerConnectionMetadataMap.put(producerVms.hashCode(), connMetadata);
                LOGGER.log(INFO,me.identifier+": Setting up consumption from producer "+producerVms);
            }

            this.channel.read(this.buffer, 0, new VmsReadCompletionHandler(producerVms, connMetadata, this.buffer));
        }

        private void processUnknownNodeType(byte nodeTypeIdentifier) {
            LOGGER.log(WARNING,me.identifier+": Presentation message from unknown source:" + nodeTypeIdentifier);
            this.buffer.clear();
            MemoryManager.releaseTemporaryDirectBuffer(this.buffer);
            try {
                this.channel.close();
            } catch (IOException ignored) { }
        }

        @Override
        public void failed(Throwable exc, Void void_) {
            LOGGER.log(WARNING,"Error on processing presentation message!");
        }
    }

    private final class AcceptCompletionHandler implements CompletionHandler<AsynchronousSocketChannel, Void> {
        @Override
        public void completed(AsynchronousSocketChannel channel, Void void_) {
            LOGGER.log(DEBUG,me.identifier+": An unknown host has started a connection attempt.");
            final ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize);
            try {
                NetworkUtils.configure(channel, options.soBufferSize);
                channel.read(buffer, null, new UnknownNodeReadCompletionHandler(channel, buffer));
            } catch(Exception e){
                LOGGER.log(ERROR,me.identifier+": Accept handler caught an exception:\n"+e);
                buffer.clear();
                MemoryManager.releaseTemporaryDirectBuffer(buffer);
            } finally {
                LOGGER.log(DEBUG,me.identifier+": Accept handler set up again for listening to new connections");
                serverSocket.accept(null, this);
            }
        }

        @Override
        public void failed(Throwable exc, Void attachment) {
            String message = exc.getMessage();
            boolean logError = true;
            if(message == null){
                if (exc.getCause() instanceof ClosedChannelException){
                    message = "Connection is closed";
                } else if ( exc instanceof AsynchronousCloseException || exc.getCause() instanceof AsynchronousCloseException) {
                    message = "Event handler has been stopped?";
                } else {
                    message = "No cause identified";
                }
                LOGGER.log(WARNING, me.identifier + ": Error on accepting connection: " + message);
            } else if(message.equalsIgnoreCase("Too many open files")){
                logError = false;
                System.out.println("Too many open files error was caught. Cannot log the error appropriately.");
            }

            if (serverSocket.isOpen()){
                serverSocket.accept(null, this);
            } else if(logError) {
                LOGGER.log(WARNING,me.identifier+": Socket is not open anymore. Cannot set up accept again");
            }

        }
    }

    private final class ConnectionFromLeaderProtocol {
        private State state;
        private final AsynchronousSocketChannel channel;
        private final ByteBuffer buffer;
        public final CompletionHandler<Integer, Void> writeCompletionHandler;

        public ConnectionFromLeaderProtocol(AsynchronousSocketChannel channel, ByteBuffer buffer) {
            this.state = State.PRESENTATION_RECEIVED;
            this.channel = channel;
            this.writeCompletionHandler = new WriteCompletionHandler();
            this.buffer = buffer;
        }

        private enum State {
            PRESENTATION_RECEIVED,
            PRESENTATION_PROCESSED,
            PRESENTATION_SENT
        }

        private final class WriteCompletionHandler implements CompletionHandler<Integer,Void> {

            @Override
            public void completed(Integer result, Void attachment) {
                state = State.PRESENTATION_SENT;
                LOGGER.log(INFO,me.identifier+": Message sent to Leader successfully = "+state);
                leaderWorker = new LeaderWorker(me, leader,
                        leaderConnectionMetadata.channel,
                        MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize));
                LOGGER.log(INFO,me.identifier+": Leader worker set up");
                buffer.clear();
                channel.read(buffer, 0, new LeaderReadCompletionHandler(leaderConnectionMetadata, buffer) );
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                LOGGER.log(INFO,me.identifier+": Failed to send presentation to Leader");
                buffer.clear();
                if(!channel.isOpen()) {
                    leaderWorker.stop();
                    leader.off();
                }
            }
        }

        public void processLeaderPresentation() {
            LOGGER.log(INFO,me.identifier+": Start processing the Leader presentation");
            boolean includeMetadata = this.buffer.get() == Presentation.YES;
            leader = Presentation.readServer(this.buffer);
            boolean hasQueuesToSubscribe = this.buffer.get() == Presentation.YES;
            if(hasQueuesToSubscribe){
                queuesLeaderSubscribesTo.addAll(Presentation.readQueuesToSubscribeTo(this.buffer, serdesProxy));
            }
            if(leaderConnectionMetadata != null) {
                LOGGER.log(WARNING, me.identifier+": Updating leader connection metadata due to new connection");
            }
            leaderConnectionMetadata = new ConnectionMetadata(
                    leader.hashCode(),
                    ConnectionMetadata.NodeType.SERVER,
                    channel
            );
            leader.on();
            this.buffer.clear();
            if(includeMetadata) {
                String vmsDataSchemaStr = serdesProxy.serializeDataSchema(me.dataSchema);
                String vmsInputEventSchemaStr = serdesProxy.serializeEventSchema(me.inputEventSchema);
                String vmsOutputEventSchemaStr = serdesProxy.serializeEventSchema(me.outputEventSchema);
                Presentation.writeVms(this.buffer, me, me.identifier, me.batch, 0, me.previousBatch, vmsDataSchemaStr, vmsInputEventSchemaStr, vmsOutputEventSchemaStr);
            } else {
                Presentation.writeVms(this.buffer, me, me.identifier, me.batch, 0, me.previousBatch);
            }
            this.buffer.flip();
            this.state = State.PRESENTATION_PROCESSED;
            LOGGER.log(INFO,me.identifier+": Message successfully received from the Leader  = "+state);
            this.channel.write( this.buffer, null, this.writeCompletionHandler );
        }
    }

    private InboundEvent buildInboundEvent(TransactionEvent.Payload payload){
        Class<?> clazz = this.vmsMetadata.queueToEventMap().get(payload.event());
        Object input = this.serdesProxy.deserialize(payload.payload(), clazz);
        Map<String, Long> precedenceMap = this.serdesProxy.deserializeDependenceMap(payload.precedenceMap());
        if(precedenceMap == null){
            throw new IllegalStateException("Precedence map is null.");
        }
        if(!precedenceMap.containsKey(this.me.identifier)){
            throw new IllegalStateException("Precedent tid of "+payload.tid()+" is unknown.");
        }
        this.tidToPrecedenceMap.put(payload.tid(), precedenceMap);
        return new InboundEvent( payload.tid(), precedenceMap.get(this.me.identifier), payload.batch(), payload.event(), clazz, input );
    }

    private static final ConcurrentLinkedDeque<List<InboundEvent>> LIST_BUFFER = new ConcurrentLinkedDeque<>();

    private final class LeaderReadCompletionHandler implements CompletionHandler<Integer, Integer> {

        private final ConnectionMetadata connectionMetadata;
        private final ByteBuffer readBuffer;

        public LeaderReadCompletionHandler(ConnectionMetadata connectionMetadata, ByteBuffer readBuffer){
            this.connectionMetadata = connectionMetadata;
            this.readBuffer = readBuffer;
            LIST_BUFFER.add(new ArrayList<>(1024));
        }

        @Override
        public void completed(Integer result, Integer startPos) {
            if(result == -1){
                LOGGER.log(INFO,me.identifier+": Leader has disconnected");
                leader.off();
                try {
                    this.connectionMetadata.channel.close();
                } catch (IOException e) {
                    e.printStackTrace(System.out);
                }
                return;
            }
            if(startPos == 0){
                this.readBuffer.flip();
                LOGGER.log(DEBUG,me.identifier+": Leader has sent "+this.readBuffer.limit()+" bytes");
            }
            byte messageType = this.readBuffer.get();
            try {
                switch (messageType) {
                    case (BATCH_OF_EVENTS) -> {
                        int bufferSize = this.getBufferSize();
                        if(this.readBuffer.remaining() < bufferSize){
                            this.fetchMoreBytes(startPos);
                            return;
                        }
                        this.processBatchOfEvents(this.readBuffer);
                    }
                    case (EVENT) -> {
                        int bufferSize = this.getBufferSize();
                        if(this.readBuffer.remaining() < bufferSize){
                            this.fetchMoreBytes(startPos);
                            return;
                        }
                        this.processSingleEvent(readBuffer);
                    }
                    case (BATCH_COMMIT_INFO) -> {
                        if(this.readBuffer.remaining() < (BatchCommitInfo.SIZE - 1)){
                            this.fetchMoreBytes(startPos);
                            return;
                        }
                        BatchCommitInfo.Payload bPayload = BatchCommitInfo.read(this.readBuffer);
                        LOGGER.log(DEBUG, me.identifier + ": Batch (" + bPayload.batch() + ") commit info received from the leader");
                        this.processNewBatchInfo(bPayload);
                    }
                    case (BATCH_COMMIT_COMMAND) -> {
                        if(this.readBuffer.remaining() < (BatchCommitCommand.SIZE - 1)){
                            this.fetchMoreBytes(startPos);
                            return;
                        }
                        BatchCommitCommand.Payload payload = BatchCommitCommand.read(this.readBuffer);
                        LOGGER.log(DEBUG, me.identifier + ": Batch (" + payload.batch() + ") commit command received from the leader");
                        this.processNewBatchCommand(payload);
                    }
                    case (TX_ABORT) -> {
                        if(this.readBuffer.remaining() < (TransactionAbort.SIZE - 1)){
                            this.fetchMoreBytes(startPos);
                            return;
                        }
                        TransactionAbort.Payload txAbortPayload = TransactionAbort.read(this.readBuffer);
                        LOGGER.log(WARNING, "Transaction (" + txAbortPayload.batch() + ") abort received from the leader?");
                        vmsInternalChannels.transactionAbortInputQueue().add(txAbortPayload);
                    }
                    case (BATCH_ABORT_REQUEST) -> {
                        if(this.readBuffer.remaining() < (BatchAbortRequest.SIZE - 1)){
                            this.fetchMoreBytes(startPos);
                            return;
                        }
                        BatchAbortRequest.Payload batchAbortReq = BatchAbortRequest.read(this.readBuffer);
                        LOGGER.log(WARNING, "Batch (" + batchAbortReq.batch() + ") abort received from the leader");
                    }
                    case (CONSUMER_SET) -> {
                        try {
                            LOGGER.log(INFO, me.identifier + ": Consumer set received from the leader");
                            Map<String, List<IdentifiableNode>> receivedConsumerVms = ConsumerSet.read(this.readBuffer, serdesProxy);
                            if (!receivedConsumerVms.isEmpty()) {
                                connectToReceivedConsumerSet(receivedConsumerVms);
                            } else {
                                LOGGER.log(WARNING, me.identifier + ": Consumer set is empty");
                            }
                        } catch (IOException e) {
                            LOGGER.log(ERROR, me.identifier + ": IOException while reading consumer set: " + e);
                            e.printStackTrace(System.out);
                        }
                    }
                    case (PRESENTATION) ->
                            LOGGER.log(WARNING, me.identifier + ": Presentation being sent again by the leader!?");
                    default ->
                            LOGGER.log(ERROR, me.identifier + ": Message type sent by the leader cannot be identified: " + messageType);
                }
            } catch (Exception e){
                LOGGER.log(ERROR, "Leader: Error caught\n"+e.getMessage(), e);
                e.printStackTrace(System.out);
            }

            if(this.readBuffer.hasRemaining()){
                this.completed(result, this.readBuffer.position());
            } else {
                this.setUpNewRead();
            }
        }

        private int getBufferSize() {
            int bufferSize = Integer.MAX_VALUE;
            if(this.readBuffer.remaining() > Integer.BYTES) {
                bufferSize = this.readBuffer.getInt();
                bufferSize -= 1 + Integer.BYTES;
            }
            return bufferSize;
        }

        private void fetchMoreBytes(Integer startPos) {
            this.readBuffer.position(startPos);
            this.readBuffer.compact();
            this.connectionMetadata.channel.read(this.readBuffer, 0, this);
        }

        private void setUpNewRead() {
            this.readBuffer.clear();
            this.connectionMetadata.channel.read(this.readBuffer, 0, this);
        }

        private void processBatchOfEvents(ByteBuffer readBuffer) {
            List<InboundEvent> payloads = LIST_BUFFER.poll();
            if(payloads == null) payloads = new ArrayList<>(1024);
            TransactionEvent.Payload payload;
            try {
                int count = readBuffer.getInt();
                LOGGER.log(DEBUG,me.identifier + ": Batch of [" + count + "] events received from the leader");
                for (int i = 0; i < count; i++) {
                    payload = TransactionEvent.read(readBuffer);
                    LOGGER.log(DEBUG, me.identifier+": Processed TID "+payload.tid());
                    if (vmsMetadata.queueToEventMap().containsKey(payload.event())) {
                        payloads.add(buildInboundEvent(payload));
                        continue;
                    }
                    LOGGER.log(WARNING,me.identifier + ": queue not identified for event received from the leader \n"+payload);
                }
                vmsInternalChannels.transactionInputQueue().addAll(payloads);
            } catch (Exception e){
                LOGGER.log(ERROR, me.identifier +": Error while processing a batch\n"+e);
                e.printStackTrace(System.out);
                if(e instanceof BufferUnderflowException) {
                    throw new RuntimeException(e);
                }
            } finally {
                payloads.clear();
                LIST_BUFFER.add(payloads);
            }
        }

        private void processSingleEvent(ByteBuffer readBuffer) {
            try {
                TransactionEvent.Payload payload = TransactionEvent.read(readBuffer);
                LOGGER.log(DEBUG,me.identifier + ": 1 event received from the leader \n"+payload);
                if (vmsMetadata.queueToEventMap().containsKey(payload.event())) {
                    InboundEvent event = buildInboundEvent(payload);
                    vmsInternalChannels.transactionInputQueue().add(event);
                    return;
                }
                LOGGER.log(WARNING,me.identifier + ": queue not identified for event received from the leader \n"+payload);
            } catch (Exception e) {
                if(e instanceof BufferUnderflowException)
                    LOGGER.log(ERROR,me.identifier + ": Buffer underflow exception while reading event: " + e);
                else
                    LOGGER.log(ERROR,me.identifier + ": Unknown exception: " + e);
            }
        }

        private void processNewBatchInfo(BatchCommitInfo.Payload batchCommitInfo){
            BatchContext batchContext = BatchContext.build(batchCommitInfo);
            batchContextMap.put(batchCommitInfo.batch(), batchContext);
            if(trackingBatchMap.containsKey(batchCommitInfo.batch())
                    && trackingBatchMap.get(batchCommitInfo.batch()).numberTIDsExecuted == batchCommitInfo.numberOfTIDsBatch()){
                LOGGER.log(INFO,me.identifier+": Requesting leader worker to send batch ("+batchCommitInfo.batch()+") complete (LATE)");
                leaderWorker.queueMessage(BatchComplete.of(batchCommitInfo.batch(), me.identifier));
            }
        }

        private void processNewBatchCommand(BatchCommitCommand.Payload batchCommitCommand){
            BatchContext batchContext = BatchContext.build(batchCommitCommand);
            batchContextMap.put(batchCommitCommand.batch(), batchContext);
            BatchMetadata batchMetadata = trackingBatchMap.get(batchCommitCommand.batch());
            if(batchMetadata == null){
                LOGGER.log(WARNING,me.identifier+": Cannot find tracking of batch "+ batchCommitCommand.batch());
                return;
            }
            if(batchContext.numberOfTIDsBatch != batchMetadata.numberTIDsExecuted) {
                LOGGER.log(WARNING,me.identifier+": Batch "+ batchCommitCommand.batch()+" has not yet finished!");
                return;
            }
            LOGGER.log(DEBUG, me.identifier + ": All TIDs for the batch " + batchCommitCommand.batch() + " have been executed");
            batchContext.setStatus(BatchContext.BATCH_COMPLETED);
            if(options.checkpointing()){
                LOGGER.log(DEBUG, me.identifier + ": Requesting checkpoint for batch " + batchCommitCommand.batch());
                batchContext.setStatus(BatchContext.CHECKPOINTING);
                submitBackgroundTask(()->checkpoint(batchCommitCommand.batch(), batchMetadata.maxTidExecuted));
            } else {
                submitBackgroundTask(()->transactionManager.cleanup(batchMetadata.maxTidExecuted));
            }
            cleanUpBatchInfo(batchCommitCommand.batch());
        }

        @Override
        public void failed(Throwable exc, Integer carryOn) {
            LOGGER.log(ERROR,me.identifier+": Message could not be processed: "+exc);
            exc.printStackTrace(System.out);
            this.setUpNewRead();
        }
    }

    public void close() {
        this.stop();
        for(var consumer : this.consumerVmsContainerMap.entrySet()){
            consumer.getValue().stop();
        }
        try {
            this.serverSocket.close();
        } catch (IOException ignored){ }
    }
}