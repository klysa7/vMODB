package dk.ku.di.dms.vms.sdk.embed.handler;

import dk.ku.di.dms.vms.modb.common.memory.MemoryManager;
import dk.ku.di.dms.vms.modb.common.schema.network.batch.*;
import dk.ku.di.dms.vms.modb.common.schema.network.control.ConsumerSet;
import dk.ku.di.dms.vms.modb.common.schema.network.control.Presentation;
import dk.ku.di.dms.vms.modb.common.schema.network.node.IdentifiableNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.ServerNode;
import dk.ku.di.dms.vms.modb.common.schema.network.node.VmsNode;
import dk.ku.di.dms.vms.modb.common.schema.network.query.QueryRequestEvent;
import dk.ku.di.dms.vms.modb.common.schema.network.query.QueryResultEvent;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionAbort;
import dk.ku.di.dms.vms.modb.common.schema.network.transaction.TransactionEvent;
import dk.ku.di.dms.vms.modb.common.serdes.IVmsSerdesProxy;
import dk.ku.di.dms.vms.modb.common.transaction.ITransactionManager;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
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
import java.nio.ByteOrder;
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
        public final int joinColumnIndex;
        public final Map<Integer, byte[]> broadcastBuffer = new ConcurrentHashMap<>();

        public JoinContext(QueryRequestEvent.QueryPayload payload, AsynchronousSocketChannel gatewayChannel, int joinColumnIndex) {
            this.payload = payload;
            this.gatewayChannel = gatewayChannel;
            this.joinColumnIndex = joinColumnIndex;
        }
    }

    public record ColRefDTO(int columnPosition) {}
    public record PredicateDTO(ColRefDTO columnReference, String expression, Object value) {}

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
        private final ByteBuffer readBuffer;

        public BroadcastReceiverHandler(AsynchronousSocketChannel channel, ByteBuffer readBuffer) {
            this.channel = channel;
            this.readBuffer = readBuffer;
        }

        @Override
        public void completed(Integer result, Integer attachment) {
            if (result == -1) return;

            // =================================================================
            // 🔥 FIX: ALWAYS FLIP BEFORE READING!
            // When an async read finishes, we must flip it so we can parse it.
            // =================================================================
            readBuffer.flip();

            while (readBuffer.hasRemaining()) {
                readBuffer.mark();
                byte type = readBuffer.get();

                if (type == END_OF_STREAM_TYPE) {
                    if (readBuffer.remaining() < 12) {
                        readBuffer.reset();
                        break;
                    }
                    readBuffer.getInt(); // skip length
                    long queryId = readBuffer.getLong();

                    JoinContext ctx = activeJoins.remove(queryId);
                    if (ctx != null) {
                        System.out.println(">>> [ORDER VMS] Broadcast END received. Hash Map size: " + ctx.broadcastBuffer.size());
                        System.out.println(">>> [ORDER VMS] Starting Join with local table: " + ctx.payload.tableName());

                        TransactionManager tm = (TransactionManager) transactionManager;
                        Iterator<byte[]> joinIter = tm.getJoinIterator(ctx.payload.tableName(), ctx.broadcastBuffer, ctx.joinColumnIndex);
                        VmsQueryWorker worker = new VmsQueryWorker(
                                ctx.gatewayChannel, joinIter, ctx.payload, options.networkBufferSize, options.networkSendTimeout
                        );
                        Thread.ofPlatform().name("query-worker-join-" + queryId).start(worker);
                    }
                    try { channel.close(); } catch (Exception ignored) {}
                    return;
                }

                if (type == QUERY_RESULT_TYPE) {
                    if (readBuffer.remaining() < 12) {
                        readBuffer.reset();
                        break;
                    }

                    int dataSize = readBuffer.getInt();

                    if (readBuffer.remaining() < dataSize) {
                        readBuffer.reset();
                        break;
                    }

                    long queryId = readBuffer.getLong();
                    JoinContext ctx = activeJoins.get(queryId);

                    int bytesRemaining = dataSize - 8;
                    while (bytesRemaining > 0) {
                        int rowSize = readBuffer.getInt();
                        byte[] rowData = new byte[rowSize];
                        readBuffer.get(rowData);
                        bytesRemaining -= (4 + rowSize);

                        if (ctx != null) {
                            if (ctx.broadcastBuffer.size() == 0) {
                                System.out.println(">>> [ORDER VMS] First broadcast packet received! Hash Join is now active.");
                            }

                            // ==============================================================
                            // 🔥 FIX: vMODB physical memory is LITTLE_ENDIAN!
                            // Let's try offset 17 (first column), 21 (second), and 25 (third)
                            // ==============================================================
                            ByteBuffer wrapper = ByteBuffer.wrap(rowData).order(ByteOrder.LITTLE_ENDIAN);
                            int c_w_id = wrapper.getInt(17);
                            int c_d_id = wrapper.getInt(21);
                            int c_id = wrapper.getInt(25);

                            if (ctx.broadcastBuffer.size() < 3) {
                                System.out.println(">>> [DEBUG JOIN] Extracted Customer row. c_w_id=" + c_w_id + ", c_d_id=" + c_d_id + ", c_id=" + c_id);
                            }

                            // We are joining on c_id (offset 25).
                            ctx.broadcastBuffer.put(Integer.hashCode(c_id), rowData);
                        }
                    }
                } else {
                    // Invalid memory read, break out to prevent spinning
                    readBuffer.reset();
                    break;
                }
            }

            // =================================================================
            // 🔥 FIX: PROPERLY PREPARE THE BUFFER FOR THE NEXT ASYNC READ
            // =================================================================
            if (readBuffer.hasRemaining()) {
                readBuffer.compact(); // Moves leftover partial messages to the front
            } else {
                readBuffer.clear(); // Empty buffer, reset to 0
            }
            // Passing '0' here is just a dummy attachment.
            channel.read(readBuffer, 0, this);
        }

        @Override
        public void failed(Throwable exc, Integer attachment) {
            System.out.println("Broadcast receive failed");
            exc.printStackTrace();
        }
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
            try {
                remoteAddress = channel.getRemoteAddress().toString();
            } catch (IOException ignored) { }

            if(result <= 0){
                LOGGER.log(WARNING, me.identifier + ": Node ("+remoteAddress+") disconnected during handshake.");
                try { this.channel.close(); } catch (IOException ignored) {}
                return;
            }

            byte messageIdentifier = this.buffer.get(0);
            LOGGER.log(INFO, ">>> [VMS HANDSHAKE] Incoming connection from " + remoteAddress + " | First Byte: " + messageIdentifier);

            if (messageIdentifier == QUERY_RESULT_TYPE || messageIdentifier == QueryRequestEvent.QUERY_REQUEST_TYPE) {
                LOGGER.log(INFO, ">>> [VMS HANDSHAKE] Direct Query detected! Bypassing Presentation.");

                // =========================================================================================
                // 🔥 FIX: We must pass 'result' directly into handler.completed()
                // Using channel.read() here would block forever waiting for new bytes from the network!
                // =========================================================================================
                if (messageIdentifier == QueryRequestEvent.QUERY_REQUEST_TYPE) {
                    ConnectionMetadata dummyMeta = new ConnectionMetadata("gateway".hashCode(), ConnectionMetadata.NodeType.GATEWAY, this.channel);
                    IdentifiableNode dummyNode = new IdentifiableNode("gateway", "localhost", 8095);
                    GatewayReadCompletionHandler handler = new GatewayReadCompletionHandler(dummyNode, dummyMeta, this.buffer);
                    handler.completed(result, 0);
                } else {
                    BroadcastReceiverHandler handler = new BroadcastReceiverHandler(this.channel, this.buffer);
                    handler.completed(result, 0);
                }
                return;
            }

            if(messageIdentifier != PRESENTATION){
                this.buffer.flip();
                String request = StandardCharsets.UTF_8.decode(this.buffer).toString();
                if(HttpUtils.isHttpClient(request)){
                    HttpReadCompletionHandler readCompletionHandler = new HttpReadCompletionHandler(
                            new ConnectionMetadata("http_client".hashCode(), ConnectionMetadata.NodeType.HTTP_CLIENT, this.channel),
                            this.buffer, MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize), httpHandler);
                    try { NetworkUtils.configure(this.channel, options.soBufferSize()); } catch (IOException ignored) { }
                    readCompletionHandler.parse(new HttpReadCompletionHandler.RequestTracking());
                } else {
                    LOGGER.log(WARNING, me.identifier + ": Connecting without presentation message. Dropping.");
                    this.buffer.clear();
                    MemoryManager.releaseTemporaryDirectBuffer(this.buffer);
                    try { this.channel.close(); } catch (IOException ignored) { }
                }
                return;
            }

            byte nodeTypeIdentifier = this.buffer.get(1);
            this.buffer.position(2);
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
                LOGGER.log(INFO, ">>> [VMS HANDSHAKE FIX] Recovered " + leftoverBytes + " piggybacked bytes! Triggering immediately.");
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
            private final ByteBuffer readBuffer;

            public GatewayReadCompletionHandler(IdentifiableNode gateway, ConnectionMetadata connectionMetadata, ByteBuffer readBuffer) {
                this.gateway = gateway;
                this.connectionMetadata = connectionMetadata;
                this.readBuffer = readBuffer;
            }

            @Override
            public void completed(Integer result, Integer startPos) {
                if (result == -1) {
                    LOGGER.log(WARNING, me.identifier + ": Gateway disconnected.");
                    try { connectionMetadata.channel.close(); } catch (IOException ignored) {}
                    return;
                }

                if (startPos == 0) readBuffer.flip();

                LOGGER.log(INFO, ">>> [VMS] Received raw message from Gateway. Size: " + readBuffer.remaining() + " bytes");

                byte type = readBuffer.get();

                if (type == QueryRequestEvent.QUERY_REQUEST_TYPE) {
                    try {
                        processQueryRequest(readBuffer);
                    } catch (Exception e) {
                        LOGGER.log(ERROR, ">>> [VMS] FATAL ERROR processing QueryRequest!", e);
                    }
                } else {
                    LOGGER.log(ERROR, ">>> [VMS] Unknown message type from Gateway: " + type);
                    readBuffer.position(readBuffer.limit());
                }

                if (readBuffer.hasRemaining()) {
                    this.completed(result, readBuffer.position());
                } else {
                    readBuffer.clear();
                    connectionMetadata.channel.read(readBuffer, 0, this);
                }
            }

            private void processQueryRequest(java.nio.ByteBuffer buffer) {
                try {
                    buffer.getInt(); // Skip length
                    var payload = QueryRequestEvent.read(buffer);
                    LOGGER.log(INFO, ">>> [VMS] Received Request | Table: " + payload.tableName() + " | Mode: " + payload.mode());

                    TransactionManager transactionManagerGateway = (TransactionManager) transactionManager;

                    List<TransactionManager.SimplePredicate> predicates = null;
                    if (payload.predicates() != null && payload.predicates().length > 0) {
                        String jsonString = new String(payload.predicates(), StandardCharsets.UTF_8);
                        PredicateDTO[] dtos = (PredicateDTO[]) serdesProxy.deserialize(jsonString, PredicateDTO[].class);

                        if (dtos != null) {
                            predicates = new ArrayList<>();
                            for (PredicateDTO dto : dtos) {
                                dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum expr =
                                        dk.ku.di.dms.vms.modb.api.query.enums.ExpressionTypeEnum.valueOf(dto.expression());
                                predicates.add(new TransactionManager.SimplePredicate(
                                        dto.columnReference().columnPosition(), expr, dto.value()));
                            }
                        }
                    }

                    final List<TransactionManager.SimplePredicate> finalPredicates = predicates;

                    if (payload.mode() == QueryRequestEvent.MODE_SCAN_TO_GATEWAY) {
                        executeStandardScan(payload, finalPredicates, transactionManagerGateway, (AsynchronousSocketChannel) connectionMetadata.channel);
                    }
                    else if (payload.mode() == QueryRequestEvent.MODE_BROADCAST_TO_VMS) {
                        String targetAddress = new String(payload.routingData(), StandardCharsets.UTF_8);
                        String[] parts = targetAddress.split(":");
                        String host = parts[0];
                        int port = Integer.parseInt(parts[1]);

                        LOGGER.log(INFO, ">>> [WAREHOUSE] Triggered! Attempting to connect to Order VMS at: " + host + ":" + port);

                        AsynchronousSocketChannel targetChannel = AsynchronousSocketChannel.open(group);

                        targetChannel.connect(new java.net.InetSocketAddress(host, port), null, new CompletionHandler<Void, Void>() {
                            @Override
                            public void completed(Void result, Void attachment) {
                                LOGGER.log(INFO, ">>> [WAREHOUSE] CONNECTION SUCCESS! Starting Scan...");
                                executeStandardScan(payload, finalPredicates, transactionManagerGateway, targetChannel);
                            }
                            @Override
                            public void failed(Throwable exc, Void attachment) {
                                LOGGER.log(ERROR, ">>> [WAREHOUSE] CONNECTION FAILED! Could not connect to Order VMS!", exc);
                            }
                        });
                    }
                    else if (payload.mode() == QueryRequestEvent.MODE_RECEIVE_AND_JOIN) {
                        int localJoinColumn = Integer.parseInt(new String(payload.routingData(), StandardCharsets.UTF_8));
                        activeJoins.put(payload.queryId(), new JoinContext(payload, (AsynchronousSocketChannel) connectionMetadata.channel, localJoinColumn));
                        LOGGER.log(INFO, ">>> [ORDER VMS] JOIN MODE active. Awaiting broadcast...");
                    }

                } catch (Exception e) {
                    LOGGER.log(ERROR, "Error processing query request", e);
                }
            }

            private void executeStandardScan(QueryRequestEvent.QueryPayload payload, List<TransactionManager.SimplePredicate> predicates, TransactionManager tm, AsynchronousSocketChannel outputChannel) {                Object indexObj = tm.getIndex(payload.tableName());
                Iterator<Long> addressIterator = tm.getScanIterator(payload.tableName(), predicates);

                if (indexObj == null || addressIterator == null) return;

                UniqueHashBufferIndex index = (UniqueHashBufferIndex) indexObj;

                VmsQueryWorker worker = new VmsQueryWorker(
                        outputChannel,
                        index,
                        addressIterator,
                        payload,
                        options.networkBufferSize(),
                        options.networkSendTimeout()
                );

                Thread.ofPlatform().name("query-worker-" + payload.queryId()).start(worker);
            }

            @Override
            public void failed(Throwable exc, Integer attachment) {
                LOGGER.log(ERROR, "Gateway connection error", exc);
            }
        }

        private void processServerPresentation() {
            if(!leader.isActive()) {
                ConnectionFromLeaderProtocol connectionFromLeader = new ConnectionFromLeaderProtocol(this.channel, this.buffer);
                connectionFromLeader.processLeaderPresentation();
            } else {
                this.buffer.get();
                ServerNode serverNode = Presentation.readServer(this.buffer);
                if(serverNode.asInetSocketAddress().equals(leader.asInetSocketAddress())) {
                    this.buffer.clear();
                    channel.read(buffer, 0, new LeaderReadCompletionHandler(new ConnectionMetadata(leader.hashCode(), ConnectionMetadata.NodeType.SERVER, channel), buffer));
                } else {
                    try { this.channel.close(); } catch (Exception ignored) {}
                }
            }
        }

        private void processVmsPresentation() {
            VmsNode producerVms = Presentation.readVms(this.buffer, serdesProxy);
            this.buffer.clear();

            ConnectionMetadata connMetadata = new ConnectionMetadata(
                    producerVms.hashCode(), ConnectionMetadata.NodeType.VMS, this.channel);

            if(!producerConnectionMetadataMap.containsKey(producerVms.hashCode())) {
                producerConnectionMetadataMap.put(producerVms.hashCode(), connMetadata);
            }
            this.channel.read(this.buffer, 0, new VmsReadCompletionHandler(producerVms, connMetadata, this.buffer));
        }

        private void processUnknownNodeType(byte nodeTypeIdentifier) {
            this.buffer.clear();
            MemoryManager.releaseTemporaryDirectBuffer(this.buffer);
            try { this.channel.close(); } catch (IOException ignored) { }
        }

        @Override
        public void failed(Throwable exc, Void void_) {
            LOGGER.log(WARNING,"Error on processing presentation message!");
        }
    }

    private final class AcceptCompletionHandler implements CompletionHandler<AsynchronousSocketChannel, Void> {
        @Override
        public void completed(AsynchronousSocketChannel channel, Void void_) {
            final ByteBuffer buffer = MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize);
            try {
                NetworkUtils.configure(channel, options.soBufferSize);
                channel.read(buffer, null, new UnknownNodeReadCompletionHandler(channel, buffer));
            } catch(Exception e){
                buffer.clear();
                MemoryManager.releaseTemporaryDirectBuffer(buffer);
            } finally {
                serverSocket.accept(null, this);
            }
        }

        @Override
        public void failed(Throwable exc, Void attachment) {
            if (serverSocket.isOpen()) serverSocket.accept(null, this);
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

        private enum State { PRESENTATION_RECEIVED, PRESENTATION_PROCESSED, PRESENTATION_SENT }

        private final class WriteCompletionHandler implements CompletionHandler<Integer,Void> {
            @Override
            public void completed(Integer result, Void attachment) {
                state = State.PRESENTATION_SENT;
                leaderWorker = new LeaderWorker(me, leader, leaderConnectionMetadata.channel, MemoryManager.getTemporaryDirectBuffer(options.networkBufferSize));
                buffer.clear();
                channel.read(buffer, 0, new LeaderReadCompletionHandler(leaderConnectionMetadata, buffer) );
            }
            @Override
            public void failed(Throwable exc, Void attachment) {
                buffer.clear();
                if(!channel.isOpen()) { leaderWorker.stop(); leader.off(); }
            }
        }

        public void processLeaderPresentation() {
            boolean includeMetadata = this.buffer.get() == Presentation.YES;
            leader = Presentation.readServer(this.buffer);
            boolean hasQueuesToSubscribe = this.buffer.get() == Presentation.YES;
            if(hasQueuesToSubscribe){ queuesLeaderSubscribesTo.addAll(Presentation.readQueuesToSubscribeTo(this.buffer, serdesProxy)); }

            leaderConnectionMetadata = new ConnectionMetadata(leader.hashCode(), ConnectionMetadata.NodeType.SERVER, channel);
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
            this.channel.write( this.buffer, null, this.writeCompletionHandler );
        }
    }

    private InboundEvent buildInboundEvent(TransactionEvent.Payload payload){
        Class<?> clazz = this.vmsMetadata.queueToEventMap().get(payload.event());
        Object input = this.serdesProxy.deserialize(payload.payload(), clazz);
        Map<String, Long> precedenceMap = this.serdesProxy.deserializeDependenceMap(payload.precedenceMap());
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
                leader.off();
                try { this.connectionMetadata.channel.close(); } catch (IOException e) {}
                return;
            }
            if(startPos == 0){ this.readBuffer.flip(); }

            byte messageType = this.readBuffer.get();
            try {
                switch (messageType) {
                    case (BATCH_OF_EVENTS) -> {
                        int bufferSize = this.getBufferSize();
                        if(this.readBuffer.remaining() < bufferSize){ this.fetchMoreBytes(startPos); return; }
                        this.processBatchOfEvents(this.readBuffer);
                    }
                    case (EVENT) -> {
                        int bufferSize = this.getBufferSize();
                        if(this.readBuffer.remaining() < bufferSize){ this.fetchMoreBytes(startPos); return; }
                        this.processSingleEvent(readBuffer);
                    }
                    case (BATCH_COMMIT_INFO) -> {
                        if(this.readBuffer.remaining() < (BatchCommitInfo.SIZE - 1)){ this.fetchMoreBytes(startPos); return; }
                        this.processNewBatchInfo(BatchCommitInfo.read(this.readBuffer));
                    }
                    case (BATCH_COMMIT_COMMAND) -> {
                        if(this.readBuffer.remaining() < (BatchCommitCommand.SIZE - 1)){ this.fetchMoreBytes(startPos); return; }
                        this.processNewBatchCommand(BatchCommitCommand.read(this.readBuffer));
                    }
                    case (TX_ABORT) -> {
                        if(this.readBuffer.remaining() < (TransactionAbort.SIZE - 1)){ this.fetchMoreBytes(startPos); return; }
                        vmsInternalChannels.transactionAbortInputQueue().add(TransactionAbort.read(this.readBuffer));
                    }
                    case (BATCH_ABORT_REQUEST) -> {
                        if(this.readBuffer.remaining() < (BatchAbortRequest.SIZE - 1)){ this.fetchMoreBytes(startPos); return; }
                        BatchAbortRequest.read(this.readBuffer);
                    }
                    case (CONSUMER_SET) -> {
                        try {
                            Map<String, List<IdentifiableNode>> receivedConsumerVms = ConsumerSet.read(this.readBuffer, serdesProxy);
                            if (!receivedConsumerVms.isEmpty()) connectToReceivedConsumerSet(receivedConsumerVms);
                        } catch (IOException e) {}
                    }
                    case (PRESENTATION) -> {}
                }
            } catch (Exception e){ }

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
            try {
                int count = readBuffer.getInt();
                for (int i = 0; i < count; i++) {
                    TransactionEvent.Payload payload = TransactionEvent.read(readBuffer);
                    if (vmsMetadata.queueToEventMap().containsKey(payload.event())) {
                        payloads.add(buildInboundEvent(payload));
                    }
                }
                vmsInternalChannels.transactionInputQueue().addAll(payloads);
            } catch (Exception e){
                if(e instanceof BufferUnderflowException) throw new RuntimeException(e);
            } finally {
                payloads.clear();
                LIST_BUFFER.add(payloads);
            }
        }

        private void processSingleEvent(ByteBuffer readBuffer) {
            try {
                TransactionEvent.Payload payload = TransactionEvent.read(readBuffer);
                if (vmsMetadata.queueToEventMap().containsKey(payload.event())) {
                    vmsInternalChannels.transactionInputQueue().add(buildInboundEvent(payload));
                }
            } catch (Exception e) { }
        }

        private void processNewBatchInfo(BatchCommitInfo.Payload batchCommitInfo){
            BatchContext batchContext = BatchContext.build(batchCommitInfo);
            batchContextMap.put(batchCommitInfo.batch(), batchContext);
            if(trackingBatchMap.containsKey(batchCommitInfo.batch()) && trackingBatchMap.get(batchCommitInfo.batch()).numberTIDsExecuted == batchCommitInfo.numberOfTIDsBatch()){
                leaderWorker.queueMessage(BatchComplete.of(batchCommitInfo.batch(), me.identifier));
            }
        }

        private void processNewBatchCommand(BatchCommitCommand.Payload batchCommitCommand){
            BatchContext batchContext = BatchContext.build(batchCommitCommand);
            batchContextMap.put(batchCommitCommand.batch(), batchContext);
            BatchMetadata batchMetadata = trackingBatchMap.get(batchCommitCommand.batch());
            if(batchMetadata == null || batchContext.numberOfTIDsBatch != batchMetadata.numberTIDsExecuted) return;

            batchContext.setStatus(BatchContext.BATCH_COMPLETED);
            if(options.checkpointing()){
                batchContext.setStatus(BatchContext.CHECKPOINTING);
                submitBackgroundTask(()->checkpoint(batchCommitCommand.batch(), batchMetadata.maxTidExecuted));
            } else {
                submitBackgroundTask(()->transactionManager.cleanup(batchMetadata.maxTidExecuted));
            }
            cleanUpBatchInfo(batchCommitCommand.batch());
        }

        @Override
        public void failed(Throwable exc, Integer carryOn) {
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