package dk.ku.di.dms.vms.tpcc.proxy;

import dk.ku.di.dms.vms.coordinator.Coordinator;
import dk.ku.di.dms.vms.modb.common.serdes.VmsSerdesProxyBuilder;
import dk.ku.di.dms.vms.modb.common.utils.ConfigUtils;
import dk.ku.di.dms.vms.sdk.embed.client.VmsApplication;
import dk.ku.di.dms.vms.tpcc.proxy.dataload.DataLoadUtils;
import dk.ku.di.dms.vms.tpcc.proxy.entities.District;
import dk.ku.di.dms.vms.tpcc.proxy.entities.Warehouse;
import dk.ku.di.dms.vms.tpcc.proxy.experiment.ExperimentUtils;
import dk.ku.di.dms.vms.tpcc.proxy.infra.MinimalHttpClient;
import dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants;
import dk.ku.di.dms.vms.tpcc.proxy.storage.StorageUtils;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;
import dk.ku.di.dms.vms.web_common.HttpUtils;
import org.junit.*;
import org.junit.runners.MethodSorters;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public final class TPCcWorkloadTest {

    private static final int NUM_WARE = 2;

    private static final int RUN_TIME = 10000;

    private static final int WARM_UP = 2000;

    private static final Properties PROPERTIES = ConfigUtils.loadProperties();

    private static VmsApplication WAREHOUSE_SVC;
    private static VmsApplication INVENTORY_SVC;
    private static VmsApplication ORDER_SVC;

    private static final StorageUtils.EntityMetadata METADATA;

    static {
        try {
            METADATA = StorageUtils.loadEntityMetadata();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void startTPCcServices(){
         // clean up
        File orderLineFile = dk.ku.di.dms.vms.modb.utils.StorageUtils.buildFile("order_line");
        File ordersFile = dk.ku.di.dms.vms.modb.utils.StorageUtils.buildFile("orders");
        File newOrdersFile = dk.ku.di.dms.vms.modb.utils.StorageUtils.buildFile("new_orders");

        orderLineFile.delete();
        ordersFile.delete();
        newOrdersFile.delete();

        String basePathStr = dk.ku.di.dms.vms.modb.utils.StorageUtils.getBasePath();
        Path basePath = Paths.get(basePathStr);
        try(var paths = Files
                // retrieve all files in the folder
                .walk(basePath)
                // find the log files
                .filter(path -> path.toString().contains(".llog"))) {
            for(var path : paths.toList()){
                path.toFile().delete();
            }
        } catch (IOException ignored){ }

        try {
            WAREHOUSE_SVC = dk.ku.di.dms.vms.tpcc.warehouse.Main.build();
            INVENTORY_SVC = dk.ku.di.dms.vms.tpcc.inventory.Main.build();
            ORDER_SVC = dk.ku.di.dms.vms.tpcc.order.Main.build();
        } catch (Exception e){
            throw new RuntimeException(e);
        }

        WAREHOUSE_SVC.start();
        INVENTORY_SVC.start();
        ORDER_SVC.start();
    }

    @AfterClass
    public static void closeTPCcServices(){
        ORDER_SVC.close();
        INVENTORY_SVC.close();
        WAREHOUSE_SVC.close();
    }

    @Test
    public void test_B_load_and_ingest() throws IOException {
        var tableToIndexMap = StorageUtils.mapTablesInDisk(METADATA, NUM_WARE);
        var tableInputMap = DataLoadUtils.mapTablesFromDisk(tableToIndexMap, METADATA.entityHandlerMap());
        // FIXME DataLoadUtils.ingestData(tableInputMap);
        hasDataBeenIngested();
    }

    @Test
    public void test_C_submit_workload() throws IOException {
        // mapping all the workload data is not a good idea, it overloads the Java heap
        //  perhaps it is better to iteratively load from memory. instead of list, pass an iterator to the worker
        //  link a file/worker to a warehouse, so there is no need to partition the file among workers
        var input = WorkloadUtils.mapWorkloadInputFiles(NUM_WARE);

        Assert.assertFalse(input.isEmpty());

        Coordinator coordinator = ExperimentUtils.loadCoordinator(PROPERTIES);
        int numConnected;
        do {
            numConnected = coordinator.getConnectedVMSs().size();
        } while (numConnected < 3);

        var expStats = ExperimentUtils.runExperiment(coordinator, input, RUN_TIME, WARM_UP);

        coordinator.stop();

        ExperimentUtils.writeResultsToFile(NUM_WARE, expStats, RUN_TIME, WARM_UP,
                coordinator.getOptions().getNumTransactionWorkers(), coordinator.getOptions().getBatchWindow(), coordinator.getOptions().getMaxTransactionsPerBatch());

        String host = PROPERTIES.getProperty("warehouse_host");
        int port = TPCcConstants.VMS_TO_PORT_MAP.get("warehouse");
        var serdesProxy = VmsSerdesProxyBuilder.build();
        // query get some items and assert correctness
        try(MinimalHttpClient httpClient = new MinimalHttpClient(host, port)){
            for(int i = 1; i <= TPCcConstants.NUM_DIST_PER_WARE; i++) {
                String resp2 = httpClient.sendGetRequest("district/"+i+"/1");
                var parsedResp = HttpUtils.parseRequest(resp2);
                var district = serdesProxy.deserialize(parsedResp.body(), District.class);
                Assert.assertTrue(district.d_next_o_id > 3001);
            }
        }
    }

    private static void hasDataBeenIngested() throws IOException {
        String host = PROPERTIES.getProperty("warehouse_host");
        int port = TPCcConstants.VMS_TO_PORT_MAP.get("warehouse");
        var serdesProxy = VmsSerdesProxyBuilder.build();
        try(MinimalHttpClient httpClient = new MinimalHttpClient(host, port)){
            String resp1 = httpClient.sendGetRequest("warehouse/1");
            var parsedResp1 = HttpUtils.parseRequest(resp1);
            var ware = serdesProxy.deserialize(parsedResp1.body(), Warehouse.class);
            Assert.assertEquals(1, ware.w_id);
            for(int i = 1 ; i <= TPCcConstants.NUM_DIST_PER_WARE; i++) {
                String resp2 = httpClient.sendGetRequest("district/"+i+"/1");
                var parsedResp2 = HttpUtils.parseRequest(resp2);
                var district = serdesProxy.deserialize(parsedResp2.body(), District.class);
                Assert.assertEquals(3001, district.d_next_o_id);
            }
        }
    }

}
