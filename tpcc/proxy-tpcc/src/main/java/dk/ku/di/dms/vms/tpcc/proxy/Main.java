package dk.ku.di.dms.vms.tpcc.proxy;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.modb.common.data_structure.Tuple;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.tpcc.proxy.dataload.DataLoadUtils;
import dk.ku.di.dms.vms.tpcc.proxy.experiment.ExperimentUtils;
import dk.ku.di.dms.vms.tpcc.proxy.hattrick.HATtrickMain;
import dk.ku.di.dms.vms.tpcc.proxy.infra.MinimalHttpClient;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;

public final class Main {

    private static final Properties PROPERTIES = ConfigUtils.loadProperties();

    public static void main(String[] args) throws Exception {
        String option;
        if(args.length == 0) {
            System.out.println("Select your deployment scheme: \n1 - Distributed \n2 - Local \nq - Quit\n\nYou can also set this automatically by passing 1 or 2 as an argument to the CLI.");
            option = new Scanner(System.in).nextLine();
        } else {
            option = args[0];
        }
        switch (option){
            case "1" -> loadMenu("Distributed Deployment Menu");
            case "2" -> loadLocalDeploymentMenu();
            default -> System.exit(0);
        }
    }

    private static void loadLocalDeploymentMenu() throws Exception {
        dk.ku.di.dms.vms.tpcc.inventory.Main.main(null);
        dk.ku.di.dms.vms.tpcc.order.Main.main(null);
        loadMenu("Local Deployment Menu");
    }

    private static void loadMenu(String menuType) {
        Coordinator coordinator = null;
        final int numWare = Integer.parseInt(PROPERTIES.get("num_ware").toString());
        final boolean truncate = Boolean.parseBoolean(PROPERTIES.getProperty("checkpointing_truncate"));
        List<Map<String,Iterator<Object>>> input;

        Map<String, Integer> numTxInputPerType = new HashMap<>(3);
        numTxInputPerType.put("new_order", Integer.valueOf(PROPERTIES.get("new_order_input_size").toString()));
        numTxInputPerType.put("payment", Integer.valueOf(PROPERTIES.get("payment_input_size").toString()));
        numTxInputPerType.put("order_status", Integer.valueOf(PROPERTIES.get("order_status_input_size").toString()));

        Map<String, Integer> txRatioMap = buildTransactionRatioMap();
        Tuple<Integer, String>[] txRatio = buildTransactionRatio(txRatioMap);

        ForkJoinPool pool = ForkJoinPool.commonPool();
        // 4 futures: order, warehouse, inventory, replica
        Future<?>[] futures = new Future[4];

        Scanner scanner = new Scanner(System.in);
        boolean running = true;
        while (running) {
            printMenu(menuType);
            System.out.print("Enter your choice: ");
            String choice = scanner.nextLine();
            switch (choice) {
                case "1": {
                    // Populate all 4 VMSes in parallel:
                    //   order (8003), warehouse (8001), inventory (8002), replica (8004)
                    // The replica populate pre-loads ~300K order_line rows so that
                    // A-qps measurements in Experiment II start from the same baseline
                    // as the live order VMS.
                    boolean useReplica = Boolean.parseBoolean(PROPERTIES.getProperty("use_replica", "false"));
                    futures[0] = pool.submit(() -> submitDataPopulationRequest("order", truncate));
                    futures[1] = pool.submit(() -> submitDataPopulationRequest("warehouse", truncate));
                    futures[2] = pool.submit(() -> submitDataPopulationRequest("inventory", truncate));
                    if (useReplica) {
                        futures[3] = pool.submit(() -> submitDataPopulationRequest("replica", truncate));
                    }
                    try {
                        int maxFuture = useReplica ? 3 : 2;
                        for (int i = maxFuture; i >= 0; i--) {
                            if (futures[i] != null) futures[i].get();
                        }
                    } catch(InterruptedException | ExecutionException e){
                        System.out.println("Error on PUT endpoint of one or more of the endpoints!");
                    }
                    break;
                }
                case "3":
                    System.out.println("Option 3: \"Create workload\" selected.");
                    System.out.println("Number of warehouses: "+numWare);
                    try {
                        WorkloadUtils.createWorkload(numWare, Boolean.getBoolean( PROPERTIES.get("multi_ware").toString() ), numTxInputPerType);
                    } catch (IOException e){
                        System.out.println("ERROR:\n"+e);
                    }
                    break;
                case "4":
                    System.out.println("Option 4: \"Submit workload\" selected.");

                    int numFiles = WorkloadUtils.getNumWorkloadInputFiles(numTxInputPerType);

                    if(numWare != numFiles){
                        System.out.println("Number of warehouses ("+numWare+") != Number of input files ("+numFiles+")");
                        System.out.println("Do you want to proceed? [y/n]");
                        String resp = scanner.nextLine();
                        if(resp.equalsIgnoreCase("n")){
                            break;
                        }
                    }

                    int batchWindow = Integer.parseInt(PROPERTIES.getProperty("batch_window_ms"));
                    int runTime;

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

                    input = WorkloadUtils.mapWorkloadInputFiles(numWare, txRatioMap);

                    if(coordinator == null){
                        coordinator = ExperimentUtils.loadCoordinator(PROPERTIES);
                        int numConnected;
                        do {
                            numConnected = coordinator.getConnectedVMSs().size();
                        } while (numConnected < 3);
                    }

                    try { Thread.sleep(100); } catch (InterruptedException _) { }

                    ExperimentUtils.ExperimentStats expStats = ExperimentUtils.runExperiment(coordinator, txRatio, input, runTime, warmUp);
                    ExperimentUtils.writeResultsToFile(numWare, expStats, runTime, warmUp,
                            coordinator.getOptions().getNumTransactionWorkers(), coordinator.getOptions().getBatchWindow(), coordinator.getOptions().getMaxTransactionsPerBatch(), txRatio, PROPERTIES.getProperty("logging"), PROPERTIES.getProperty("checkpointing"));
                    break;
                case "5":
                    System.out.println("Option 5: \"Cleanup VMS states\" selected.");
                    if (checkCompleteness(coordinator, scanner)) break;
                    DataLoadUtils.cleanup(false);
                    System.out.println("VMS states cleaned.");
                    break;
                case "6":
                    System.out.println("Option 5: \"Reset VMS states\" selected.");
                    if (checkCompleteness(coordinator, scanner)) break;
                    DataLoadUtils.cleanup(true);
                    System.out.println("VMS states reset.");
                    break;
                case "7":
                    HATtrickMain.run(coordinator);
                    break;
                case "q":
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

    private static void submitDataPopulationRequest(String vms, boolean truncate) {
        MinimalHttpClient client = null;
        String param = truncate ? "populate" : "load";
        try {
            client = DataLoadUtils.obtainHttpClient(vms);
            if (client.sendRequest("PUT", "", param) != 200) {
                System.out.println("Error on PUT endpoint of "+vms);
            }
        } catch (IOException e) {
            System.out.println("Error on PUT endpoint of "+vms);
        } finally {
            if(client != null) DataLoadUtils.returnHttpClient(vms, client);
        }
    }

    private static boolean checkCompleteness(Coordinator coordinator, Scanner scanner) {
        if(coordinator != null){
            long numTIDsCommitted = coordinator.getNumTIDsCommitted();
            long numTIDsSubmitted = coordinator.getNumTIDsSubmitted();
            if(numTIDsCommitted != numTIDsSubmitted){
                System.out.println("There are ongoing batches executing! Cannot reset states now. \n Number of TIDs committed: "+numTIDsCommitted+"\n Number of TIDs submitted: "+numTIDsSubmitted);
                System.out.println("Do you want to proceed? [y/n]");
                String resp = scanner.nextLine();
                return resp.equalsIgnoreCase("n");
            }
        }
        return false;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Transaction ratio validation — REVERTED from the single-100 check.
    //
    // The previous version required exactly one transaction type to equal 100,
    // which made balanced mixes (e.g. 50/50 new_order + payment) impossible.
    // This is the original behavior: ratios may be distributed across multiple
    // transaction types, and must sum to 100.
    //
    // Examples that now validate:
    //   new_order=100, payment=0,   order_status=0    → pure new_order
    //   new_order=50,  payment=50,  order_status=0    → HATtrick 50/50 (default)
    //   new_order=45,  payment=43,  order_status=12   → full TPC-C mix
    //
    // Note: HATtrickTClientWorker hardcodes its own 50/50 new_order/payment
    // split and does not read these properties. Ratios here only affect
    // Menu Options 3 (Create workload) and 4 (Submit workload).
    // ─────────────────────────────────────────────────────────────────────────
    public static Map<String, Integer> buildTransactionRatioMap(){
        Map<String, Integer> txRatioMap = new TreeMap<>();
        int total = 0;
        if(!PROPERTIES.get("new_order").toString().equals("0")) {
            int v = Integer.parseInt(PROPERTIES.get("new_order").toString());
            txRatioMap.put("new_order", v);
            total += v;
        }
        if(!PROPERTIES.get("payment").toString().equals("0")) {
            int v = Integer.parseInt(PROPERTIES.get("payment").toString());
            txRatioMap.put("payment", v);
            total += v;
        }
        if(!PROPERTIES.get("order_status").toString().equals("0")) {
            int v = Integer.parseInt(PROPERTIES.get("order_status").toString());
            txRatioMap.put("order_status", v);
            total += v;
        }
        if(total != 100) {
            throw new RuntimeException(
                    "Transaction ratios must sum to 100 in app.properties! Current sum: " + total);
        }
        return txRatioMap;
    }

    @SuppressWarnings("unchecked")
    private static Tuple<Integer, String>[] buildTransactionRatio(Map<String, Integer> txRatioMap) {
        Tuple<Integer, String>[] txRatio = new Tuple[txRatioMap.size()];
        int i = 0;
        for(var entry : txRatioMap.entrySet()) {
            txRatio[i] = Tuple.of(entry.getValue(), entry.getKey());
            i++;
        }
        return txRatio;
    }

    private static void printMenu(String menuType) {
        System.out.println("\n=== "+menuType+" ===");
        System.out.println("1. Populate VMS states");
        System.out.println("2. Check VMS state correctness");
        System.out.println("3. Create workload");
        System.out.println("4. Submit workload");
        System.out.println("5. Cleanup VMS states");
        System.out.println("6. Reset VMS states");
        System.out.println("7. HATtrick throughput frontier experiment");
        System.out.println("q. Quit program");
    }

}