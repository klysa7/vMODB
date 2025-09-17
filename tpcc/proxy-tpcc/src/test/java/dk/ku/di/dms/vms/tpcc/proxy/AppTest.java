package dk.ku.di.dms.vms.tpcc.proxy;

import dk.ku.di.dms.vms.tpcc.proxy.dataload.DataLoadUtils;
import dk.ku.di.dms.vms.tpcc.proxy.storage.StorageUtils;
import dk.ku.di.dms.vms.tpcc.proxy.workload.WorkloadUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public final class AppTest {

    private static final int NUM_WARE = 2;

    @Test
    public void testLoadAndIngest() throws Exception {
        StorageUtils.EntityMetadata metadata = StorageUtils.loadEntityMetadata();
        StorageUtils.createTables(metadata, NUM_WARE);
        var tableToIndexMap = StorageUtils.mapTablesInDisk(metadata, NUM_WARE);
        int numWare = StorageUtils.getNumRecordsFromInDiskTable(metadata.entityToSchemaMap().get("warehouse"), "warehouse");
        Assert.assertEquals(NUM_WARE, numWare);
        // init stub warehouse service
        var vms = new TestService().buildAndStart();
        // submit data to warehouse stub
        Assert.assertNotNull(DataLoadUtils.mapTablesFromDisk(tableToIndexMap, metadata.entityHandlerMap()));
        vms.close();
    }

    @Test
    public void testWorkload() throws IOException {
        // create
        WorkloadUtils.createWorkload(NUM_WARE, 100000, false, 100);
        // load
        var loaded = WorkloadUtils.mapWorkloadInputFiles(NUM_WARE);
        Assert.assertEquals(NUM_WARE, loaded.size());
    }
}
