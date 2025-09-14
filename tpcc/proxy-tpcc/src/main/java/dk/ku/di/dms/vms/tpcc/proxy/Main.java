package dk.ku.di.dms.vms.tpcc.proxy;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.modb.index.unique.UniqueHashBufferIndex;
import dk.ku.di.dms.vms.tpcc.common.events.NewOrderWareIn;
import dk.ku.di.dms.vms.tpcc.proxy.dataload.DataLoadUtils;
import dk.ku.di.dms.vms.tpcc.proxy.dataload.QueueTableIterator;
import dk.ku.di.dms.vms.tpcc.proxy.experiment.ExperimentUtils;
import dk.ku.di.dms.vms.tpcc.proxy.infra.MinimalHttpClient;
import dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants;
import dk.ku.di.dms.vms.tpcc.proxy.storage.StorageUtils;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;

import java.io.IOException;
import java.util.*;

public final class Main {

    private static final Properties PROPERTIES = ConfigUtils.loadProperties();

    private static int NUM_INGESTION_WORKERS;

    public static void main(String[] args) throws Exception {
        if(args.length == 0) {
            NUM_INGESTION_WORKERS = Runtime.getRuntime().availableProcessors();
            loadMenu("Main Menu");
        } else {
            NUM_INGESTION_WORKERS = Runtime.getRuntime().availableProcessors() / 2;
            loadMicroBenchMenu();
        }
    }

    private static void loadMicroBenchMenu() throws Exception {
        // set default values to override for all in-process VMSes
        PROPERTIES.setProperty("vms_thread_pool_size", "0");
        PROPERTIES.setProperty("network_thread_pool_size", "0");

        dk.ku.di.dms.vms.tpcc.warehouse.Main.main(null);
        dk.ku.di.dms.vms.tpcc.inventory.Main.main(null);
        dk.ku.di.dms.vms.tpcc.order.Main.main(null);

        loadMenu("Microbench Menu");
    }

    private static void loadMenu(String menuType) throws NoSuchFieldException, IllegalAccessException {
        Coordinator coordinator = null;
        int numWare = 0;
        Map<String, UniqueHashBufferIndex> tables = null;
        List<Iterator<NewOrderWareIn>> input;
        StorageUtils.EntityMetadata metadata = StorageUtils.loadEntityMetadata();

        Map<String, String> vmsToHostMap = new HashMap<>();
        vmsToHostMap.put("warehouse_host", PROPERTIES.getProperty("warehouse_host"));
        vmsToHostMap.put("inventory_host", PROPERTIES.getProperty("inventory_host"));
        vmsToHostMap.put("order_host", PROPERTIES.getProperty("order_host"));

        Scanner scanner = new Scanner(System.in);
        boolean running = true;
        while (running) {
            printMenu(menuType);
            System.out.print("Enter your choice: ");
            String choice = scanner.nextLine();
            System.out.println("You chose option: " + choice);
            switch (choice) {
                case "1":
                    System.out.println("Option 1: \"Create tables in disk\" selected.");
                    System.out.println("Enter number of warehouses: ");
                    numWare = Integer.parseInt(scanner.nextLine());
                    System.out.println("Creating tables with "+numWare+" warehouses...");
                    tables = StorageUtils.createTables(metadata, numWare);
                    System.out.println("Tables created!");
                    break;
                case "2":
                    System.out.println("Option 2: \"Load services with tables in disk\" selected.");

                    if(coordinator != null){
                        long submitted = coordinator.getNumTIDsSubmitted();
                        long committed = coordinator.getNumTIDsCommitted();
                        if(submitted > committed) {
                            System.out.println("Transactions are still executing: "+submitted+" > "+committed);
                            System.out.println("Do you want to proceed? [y/n]");
                            String resp = scanner.nextLine();
                            if(resp.equalsIgnoreCase("n")){
                                break;
                            }
                        }
                    }

                    if(tables == null) {
                        System.out.println("Loading tables from disk...");
                        // the number of warehouses must be exactly the same otherwise lead to errors in reading from files
                        numWare = StorageUtils.getNumRecordsFromInDiskTable(metadata.entityToSchemaMap().get("warehouse"), "warehouse");
                        tables = StorageUtils.mapTablesInDisk(metadata, numWare);
                    }
                    Map<String, QueueTableIterator> tablesInMem = DataLoadUtils.mapTablesFromDisk(tables, metadata.entityHandlerMap());
                    DataLoadUtils.ingestData(tablesInMem, vmsToHostMap, NUM_INGESTION_WORKERS);
                    break;
                case "3":
                    System.out.println("Option 3: \"Create workload\" selected.");

                    if(numWare == 0){
                        numWare = StorageUtils.getNumRecordsFromInDiskTable(metadata.entityToSchemaMap().get("warehouse"), "warehouse");
                    }
                    if(numWare == 0) {
                        System.out.println("Enter number of warehouses: ");
                        numWare = Integer.parseInt(scanner.nextLine());
                    }

                    System.out.println("Number of warehouses: "+numWare);

                    System.out.println("Allow multi warehouse transactions? [0/1]");
                    boolean multiWarehouses = Integer.parseInt(scanner.nextLine()) > 0;
                    System.out.println("Enter number of transactions per warehouse: ");
                    int numTxn = Integer.parseInt(scanner.nextLine());
                    WorkloadUtils.createWorkload(numWare, numTxn, multiWarehouses);
                    break;
                case "4":
                    System.out.println("Option 4: \"Submit workload\" selected.");

                    // check if workload files exist
                    int numFiles = WorkloadUtils.getNumWorkloadInputFiles();

                    if(numWare == 0){
                        numWare = StorageUtils.getNumRecordsFromInDiskTable(metadata.entityToSchemaMap().get("warehouse"), "warehouse");
                    }

                    if(numWare != numFiles){
                        System.out.println("Number of warehouses ("+numWare+") != Number of input files ("+numFiles+")");
                        System.out.println("Do you want to proceed? [y/n]");
                        String resp = scanner.nextLine();
                        if(resp.equalsIgnoreCase("n")){
                            break;
                        }
                    }

                    int batchWindow = Integer.parseInt( PROPERTIES.getProperty("batch_window_ms") );
                    int runTime;

                    /*
                    if(numWare == 0){
                        // get number of input files
                        numWare = WorkloadUtils.getNumWorkloadInputFiles();
                        if(numWare == 0){
                            // some unknown bug....
                            System.out.println("Zero warehouses identified. Falling back to warehouse table information...");
                            // fallback to table information
                            numWare = StorageUtils.getNumRecordsFromInDiskTable(metadata.entityToSchemaMap().get("warehouse"), "warehouse");
                        }
                        if(numWare == 0){
                            System.out.println("No warehouses identified! Maybe you forgot to generate?");
                            break;
                        }
                        System.out.println(numWare+" warehouses identified");
                    }
                     */

                    while(true) {
                        System.out.print("Enter duration (ms): [press 0 for 10s] ");
                        runTime = Integer.parseInt(scanner.nextLine());
                        if (runTime == 0) runTime = 10000;
                        if(runTime < (batchWindow * 2)){
                            System.out.print("Duration must be at least 2 * "+batchWindow+" (ms)\n");
                            continue;
                        }
                        break;
                    }
                    int warmUp;
                    while(true) {
                        System.out.println("Enter warm up period (ms): [press 0 for 2s] ");
                        warmUp = Integer.parseInt(scanner.nextLine());
                        if (warmUp <= 0) warmUp = 2000;
                        if(warmUp > runTime){
                            System.out.print("Warm up must be lower than run time "+runTime+" (ms)\n");
                            continue;
                        }
                        break;
                    }

                    // reload iterators
                    input = WorkloadUtils.mapWorkloadInputFiles(numWare);

                    // load coordinator
                    if(coordinator == null){
                        coordinator = ExperimentUtils.loadCoordinator(PROPERTIES);
                        // wait for all starter VMSes to connect
                        int numConnected;
                        do {
                            numConnected = coordinator.getConnectedVMSs().size();
                        } while (numConnected < 3);
                    }
                    var expStats = ExperimentUtils.runExperiment(coordinator, input, runTime, warmUp);
                    ExperimentUtils.writeResultsToFile(numWare, expStats, runTime, warmUp,
                            coordinator.getOptions().getNumTransactionWorkers(), coordinator.getOptions().getBatchWindow(), coordinator.getOptions().getMaxTransactionsPerBatch());
                    break;
                case "5":
                    System.out.println("Option 5: \"Reset service states\" selected.");
                    // has to wait for all submitted transactions to commit in order to send the reset
                    if(coordinator != null){
                        long numTIDsCommitted = coordinator.getNumTIDsCommitted();
                        long numTIDsSubmitted = coordinator.getNumTIDsSubmitted();
                        if(numTIDsCommitted != numTIDsSubmitted){
                            System.out.println("There are ongoing batches executing! Cannot reset states now. \n Number of TIDs committed: "+numTIDsCommitted+"\n Number of TIDs submitted: "+numTIDsSubmitted);
                            System.out.println("Do you want to proceed? [y/n]");
                            String resp = scanner.nextLine();
                            if(resp.equalsIgnoreCase("n")){
                                break;
                            }
                            break;
                        }
                    }
                    // cleanup service states
                    for(var vms : TPCcConstants.VMS_TO_PORT_MAP.entrySet()){
                        String host = PROPERTIES.getProperty(vms.getKey() + "_host");
                        try(var client = new MinimalHttpClient(host, vms.getValue())){
                            if(client.sendRequest("PATCH", "", "reset") != 200){
                                System.out.println("Error on resetting "+vms+" state!");
                            }
                        } catch (IOException e) {
                            System.out.println("Exception on resetting "+vms+" state: \n"+e);
                        }
                    }
                    System.out.println("Service states reset.");
                    break;
                case "0":
                    System.out.println("Exiting the application...");
                    running = false;
                    break;
                default:
                    System.out.println("Invalid choice. Please try again.");
            }
        }
        scanner.close();
        System.exit(0);
    }

    private static void printMenu(String menuType) {
        System.out.println("\n=== "+menuType+" ===");
        System.out.println("1. Create tables in disk");
        System.out.println("2. Load services with tables in disk");
        System.out.println("3. Create workload");
        System.out.println("4. Submit workload");
        System.out.println("5. Reset service states");
        System.out.println("0. Exit");
    }

}
