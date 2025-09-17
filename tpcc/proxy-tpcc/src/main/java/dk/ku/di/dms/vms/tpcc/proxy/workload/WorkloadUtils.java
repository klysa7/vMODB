package dk.ku.di.dms.vms.tpcc.proxy.workload;

import dk.ku.di.dms.vms.modb.common.constraint.ConstraintReference;
import dk.ku.di.dms.vms.modb.common.memory.MemoryUtils;
import dk.ku.di.dms.vms.modb.common.type.DataType;
import dk.ku.di.dms.vms.modb.common.type.DataTypeUtils;
import dk.ku.di.dms.vms.modb.definition.Schema;
import dk.ku.di.dms.vms.modb.storage.record.AppendOnlyBoundedBuffer;
import dk.ku.di.dms.vms.modb.storage.record.AppendOnlyUnboundedBuffer;
import dk.ku.di.dms.vms.modb.utils.StorageUtils;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;
import dk.ku.di.dms.vms.tpcc.common.events.OrderStatusIn;
import dk.ku.di.dms.vms.tpcc.proxy.datagen.DataGenUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;

import static dk.ku.di.dms.vms.tpcc.proxy.datagen.DataGenUtils.nuRand;
import static dk.ku.di.dms.vms.tpcc.proxy.datagen.DataGenUtils.randomNumber;
import static dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants.*;
import static java.lang.System.Logger.Level.ERROR;
import static java.lang.System.Logger.Level.INFO;

public final class WorkloadUtils {

    private static final System.Logger LOGGER = System.getLogger(WorkloadUtils.class.getName());

    private static final String NEW_ORDER_INPUT_BASE_FILE_NAME = "new_order_input_";

    private static final String ORDER_STATUS_INPUT_BASE_FILE_NAME = "order_status_input_";

    private static final Schema NEW_ORDER_SCHEMA = new Schema(
            new String[]{ "w_id", "d_id", "c_id", "itemIds", "supWares", "qty", "allLocal" },
            new DataType[]{
                    DataType.INT, DataType.INT, DataType.INT, DataType.INT_ARRAY, DataType.INT_ARRAY, DataType.INT_ARRAY, DataType.BOOL
            },
            new int[]{},
            new ConstraintReference[]{},
            false
    );

    private static final Schema ORDER_STATUS_SCHEMA = new Schema(
            new String[]{ "w_id", "d_id", "c_id", "c_last", "by_name" },
            new DataType[]{
                    DataType.INT, DataType.INT, DataType.INT, DataType.STRING, DataType.BOOL
            },
            new int[]{},
            new ConstraintReference[]{},
            false
    );

    private static void writeRecordInMemoryPos(long pos, Object[] record, Schema schema) {
        long currAddress = pos;
        for (int index = 0; index < schema.columnOffset().length; index++) {
            DataType dt = schema.columnDataType(index);
            DataTypeUtils.callWriteFunction(currAddress, dt, record[index]);
            currAddress += dt.value;
        }
    }

    private static Object[] readRecordFromMemoryPos(long address, Schema schema){
        Object[] record = new Object[schema.columnOffset().length];
        long currAddress = address;
        for(int i = 0; i < schema.columnOffset().length; i++) {
            DataType dt = schema.columnDataType(i);
            record[i] = DataTypeUtils.getValue(dt, currAddress);
            currAddress += dt.value;
        }
        return record;
    }

    /**
     * @param initTs Necessary to discard batches that complete after the end of the experiment
     * @param submitted Necessary to calculate the latency, throughput, and percentiles
     */
    public record WorkloadStats(long initTs, Map<Long, List<Long>>[] submitted){}

    @SuppressWarnings("unchecked")
    public static WorkloadStats submitWorkload(List<Iterator<Object>> input, Function<Object, Long> func) {
        int numWorkers = input.size();
        LOGGER.log(INFO, "Submitting transactions through "+numWorkers+" worker(s)");
        CountDownLatch allThreadsStart = new CountDownLatch(numWorkers+1);
        CountDownLatch allThreadsAreDone = new CountDownLatch(numWorkers);
        Map<Long, List<Long>>[] submittedArray = new Map[numWorkers];

        for(int i = 0; i < numWorkers; i++) {
            final Iterator<Object> workerInput = input.get(i);
            int finalI = i;
            Thread thread = new Thread(()-> submittedArray[finalI] =
                            Worker.run(allThreadsStart, allThreadsAreDone, workerInput, func));
            thread.start();
        }

        allThreadsStart.countDown();
        try {
            allThreadsStart.await();
            LOGGER.log(INFO,"Experiment main going to wait for the workers to finish.");
            allThreadsAreDone.await();
            LOGGER.log(INFO,"Experiment main woke up!");
        } catch (InterruptedException e){
            throw new RuntimeException(e);
        }

        return new WorkloadStats(System.currentTimeMillis(), submittedArray);
    }

    private static final class Worker {

        public static Map<Long, List<Long>> run(CountDownLatch allThreadsStart, CountDownLatch allThreadsAreDone,
                                                Iterator<Object> input, Function<Object, Long> func) {
            Map<Long,List<Long>> startTsMap = new HashMap<>();
            long threadId = Thread.currentThread().threadId();
            LOGGER.log(INFO,"Thread ID " + threadId + " started");
            allThreadsStart.countDown();
            try {
                allThreadsStart.await();
            } catch (InterruptedException e) {
                LOGGER.log(ERROR, "Thread ID "+threadId+" failed to await start");
                throw new RuntimeException(e);
            }
            long currentTs;
            while (input.hasNext()){
                try {
                    long batchId = func.apply(input.next());
                    currentTs = System.currentTimeMillis();
                    if(!startTsMap.containsKey(batchId)){
                        startTsMap.put(batchId, new ArrayList<>());
                    }
                    startTsMap.get(batchId).add(currentTs);
                } catch (Exception e) {
                    LOGGER.log(ERROR,"Exception in Thread ID: " + (e.getMessage() == null ? "No message" : e.getMessage()));
                    throw new RuntimeException(e);
                }
            }
            allThreadsAreDone.countDown();
            return startTsMap;
        }
    }

    public static List<Iterator<Object>> mapWorkloadInputFiles(int numWare){
        LOGGER.log(INFO, "Mapping "+numWare+" warehouse input files from disk...");
        long initTs = System.currentTimeMillis();
        List<Iterator<Object>> input = new ArrayList<>(numWare);
        for(int i = 0; i < numWare; i++){
            // new order
            AppendOnlyBoundedBuffer newOrderBuffer = StorageUtils.loadAppendOnlyBuffer(NEW_ORDER_INPUT_BASE_FILE_NAME +(i+1));
            // calculate number of entries (i.e., transaction requests)
            int numTransactions = (int) newOrderBuffer.size() / NEW_ORDER_SCHEMA.getRecordSize();
            input.add( createNewOrderInputIterator(newOrderBuffer, numTransactions) );

            // order status
            AppendOnlyBoundedBuffer buffer = StorageUtils.loadAppendOnlyBuffer(ORDER_STATUS_INPUT_BASE_FILE_NAME +(i+1));
            numTransactions = (int) buffer.size() / ORDER_STATUS_SCHEMA.getRecordSize();
            input.add( createOrderStatusInputIterator(buffer, numTransactions) );
        }
        long endTs = System.currentTimeMillis();
        LOGGER.log(INFO, "Mapped "+numWare+" warehouse input files from disk in "+(endTs-initTs)+" ms");
        return input;
    }

    private static Iterator<Object> createOrderStatusInputIterator(AppendOnlyBoundedBuffer buffer, int numTransactions){
        return new Iterator<>() {
            int txIdx = 1;
            @Override
            public boolean hasNext() {
                return this.txIdx <= numTransactions;
            }
            @Override
            public OrderStatusIn next() {
                Object[] newOrderInput = readRecordFromMemoryPos(buffer.nextOffset(), ORDER_STATUS_SCHEMA);
                buffer.forwardOffset(ORDER_STATUS_SCHEMA.getRecordSize());
                this.txIdx++;
                return parseOrderStatusRecordIntoEntity(newOrderInput);
            }
        };
    }

    private static Iterator<Object> createNewOrderInputIterator(AppendOnlyBoundedBuffer buffer, int numTransactions){
        return new Iterator<>() {
            int txIdx = 1;
            @Override
            public boolean hasNext() {
                return this.txIdx <= numTransactions;
            }
            @Override
            public NewOrderWareIn next() {
                Object[] newOrderInput = readRecordFromMemoryPos(buffer.nextOffset(), NEW_ORDER_SCHEMA);
                buffer.forwardOffset(NEW_ORDER_SCHEMA.getRecordSize());
                this.txIdx++;
                return parseNewOrderRecordIntoEntity(newOrderInput);
            }
        };
    }

    public static void createWorkload(int numWare, int numTransactions, boolean allowMultiWarehouses, int newOrderRatio) throws IOException {
        deleteWorkloadInputFiles();
        LOGGER.log(INFO, "Generating "+(numTransactions * numWare)+" transactions ("+numTransactions+" per warehouse/worker)");
        long initTs = System.currentTimeMillis();
        if(newOrderRatio == 100) {
            for (int ware = 1; ware <= numWare; ware++) {
                LOGGER.log(INFO, "Generating " + numTransactions + " transactions for warehouse " + ware);
                String fileName = NEW_ORDER_INPUT_BASE_FILE_NAME + ware;
                AppendOnlyBoundedBuffer buffer = StorageUtils.loadAppendOnlyBoundedBuffer(numTransactions, NEW_ORDER_SCHEMA.getRecordSize(), fileName, true);
                for (int txIdx = 1; txIdx <= numTransactions; txIdx++) {
                    Object[] newOrderInput = generateNewOrder(ware, numWare, allowMultiWarehouses);
                    writeRecordInMemoryPos(buffer.nextOffset(), newOrderInput, NEW_ORDER_SCHEMA);
                    buffer.forwardOffset(NEW_ORDER_SCHEMA.getRecordSize());
                }
                buffer.force();
            }
        } else {
            var random = ThreadLocalRandom.current();
            ByteBuffer newOrderNativeBuffer = ByteBuffer.allocateDirect(NEW_ORDER_SCHEMA.getRecordSize());
            long newOrderBufferAddress = MemoryUtils.getByteBufferAddress(newOrderNativeBuffer);
            ByteBuffer orderStatusNativeBuffer = ByteBuffer.allocateDirect(ORDER_STATUS_SCHEMA.getRecordSize());
            long orderStatusBufferAddress = MemoryUtils.getByteBufferAddress(orderStatusNativeBuffer);
            for (int ware = 1; ware <= numWare; ware++) {
                String newOrderInputFileName = NEW_ORDER_INPUT_BASE_FILE_NAME + ware;
                AppendOnlyUnboundedBuffer newOrderBuffer = StorageUtils.loadAppendOnlyUnboundedBuffer(newOrderInputFileName);
                String orderStatusInputFileName = ORDER_STATUS_INPUT_BASE_FILE_NAME + ware;
                AppendOnlyUnboundedBuffer orderStatusBuffer = StorageUtils.loadAppendOnlyUnboundedBuffer(orderStatusInputFileName);
                for (int txIdx = 1; txIdx <= numTransactions; txIdx++) {
                    int tx = random.nextInt(1, 101);
                    if (tx <= newOrderRatio) {
                        Object[] newOrderInput = generateNewOrder(ware, numWare, allowMultiWarehouses);
                        writeRecordInMemoryPos(newOrderBufferAddress, newOrderInput, NEW_ORDER_SCHEMA);
                        newOrderBuffer.append(newOrderNativeBuffer);
                        newOrderNativeBuffer.clear();
                    } else {
                        Object[] orderStatusInput = generateOrderStatus(ware);
                        writeRecordInMemoryPos(orderStatusBufferAddress, orderStatusInput, ORDER_STATUS_SCHEMA);
                        orderStatusBuffer.append(orderStatusNativeBuffer);
                        orderStatusNativeBuffer.clear();
                    }
                }
                newOrderBuffer.force();
                orderStatusBuffer.force();
            }
        }
        long endTs = System.currentTimeMillis();
        LOGGER.log(INFO, "Generated "+(numTransactions * numWare)+" transactions in "+(endTs-initTs)+" ms");
    }
    
    public static void deleteWorkloadInputFiles(){
        String basePathStr = StorageUtils.getBasePath();
        Path basePath = Paths.get(basePathStr);
        try(var paths = Files.walk(basePath)){
            var newOrderInputFiles = paths.filter(path -> path.toString().contains(NEW_ORDER_INPUT_BASE_FILE_NAME) || path.toString().contains(ORDER_STATUS_INPUT_BASE_FILE_NAME)).toList();
            for (var path : newOrderInputFiles){
                if(!path.toFile().delete()){
                    LOGGER.log(ERROR, "Could not dele file path: \n"+path);
                }
            }
        } catch (IOException e){
            LOGGER.log(ERROR, "Error captured while trying to access base path: \n"+e);
        }
    }

    public static int getNumWorkloadInputFiles(){
        String basePathStr = StorageUtils.getBasePath();
        Path basePath = Paths.get(basePathStr);
        try(var paths = Files.walk(basePath)){
            var workloadInputFiles = paths.filter(path -> path.toString().contains(NEW_ORDER_INPUT_BASE_FILE_NAME)).toList();
            return workloadInputFiles.size();
        } catch (IOException e){
            LOGGER.log(ERROR, "Error captured while trying to access base path: \n"+e);
            return 0;
        }
    }

    private static NewOrderWareIn parseNewOrderRecordIntoEntity(Object[] newOrderInput) {
        return new NewOrderWareIn(
                (int) newOrderInput[0],
                (int) newOrderInput[1],
                (int) newOrderInput[2],
                (int[]) newOrderInput[3],
                (int[]) newOrderInput[4],
                (int[]) newOrderInput[5],
                (boolean) newOrderInput[6]
        );
    }

    private static OrderStatusIn parseOrderStatusRecordIntoEntity(Object[] orderStatusInput) {
        return new OrderStatusIn(
                (int) orderStatusInput[0],
                (int) orderStatusInput[1],
                (int) orderStatusInput[2],
                (String) orderStatusInput[3],
                (boolean) orderStatusInput[4]
        );
    }

    private static Object[] generateOrderStatus(int w_id){
        int d_id = randomNumber(1, NUM_DIST_PER_WARE);
        int c_id = nuRand(1023, 1, NUM_CUST_PER_DIST);
        String c_last = DataGenUtils.lastName(nuRand(255,0,999));
        boolean by_name = randomNumber(1, 100) <= 60;
        return new Object[]{ w_id, d_id, c_id, c_last, by_name };
    }

    private static Object[] generateNewOrder(int w_id, int num_ware, boolean allowMultiWarehouses){
        int d_id;
        int c_id;
        int ol_cnt;
        int all_local = 1;
        int not_found = NUM_ITEMS + 1;
        int rbk;

        d_id = randomNumber(1, NUM_DIST_PER_WARE);
        c_id = nuRand(1023, 1, NUM_CUST_PER_DIST);

        ol_cnt = randomNumber(MIN_NUM_ITEMS_PER_ORDER, MAX_NUM_ITEMS_PER_ORDER);
        rbk = randomNumber(1, 100);

        int[] itemIds = new int[ol_cnt];
        int[] supWares = new int[ol_cnt];
        int[] qty = new int[ol_cnt];

        for (int i = 0; i < ol_cnt; i++) {
            int item_ = nuRand(8191, 1, NUM_ITEMS);

            // avoid duplicate items
            while(foundItem(itemIds, i, item_)){
                item_ = nuRand(8191, 1, NUM_ITEMS);
            }
            itemIds[i] = item_;

            if(FORCE_ABORTS) {
                if ((i == ol_cnt - 1) && (rbk == 1)) {
                    // this can lead to exception and then abort in app code
                    itemIds[i] = not_found;
                }
            }

            if (allowMultiWarehouses) {
                if (randomNumber(1, 100) != 1) {
                    supWares[i] = w_id;
                } else {
                    supWares[i] = otherWare(num_ware, w_id);
                    all_local = 0;
                }
            } else {
                supWares[i] = w_id;
            }
            qty[i] = randomNumber(1, 10);
        }

        return new Object[]{ w_id, d_id, c_id, itemIds, supWares, qty, all_local == 1 };
    }

    private static boolean foundItem(int[] itemIds, int length, int value){
        if(length == 0) return false;
        for(int i = 0; i < length; i++){
            if(itemIds[i] == value) return true;
        }
        return false;
    }

    /**
     * Based on <a href="https://github.com/AgilData/tpcc/blob/master/src/main/java/com/codefutures/tpcc/Driver.java#L310">AgilData</a>
     */
    private static int otherWare(int num_ware, int home_ware) {
        int tmp;
        if (num_ware == 1) return home_ware;
        do {
            tmp = randomNumber(1, num_ware);
        } while (tmp == home_ware);
        return tmp;
    }

}
