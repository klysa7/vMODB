package dk.ku.di.dms.vms.web_common.olap.dto;

public record OlapScanRequest(
        String schema,
        String table,
        long snapshotId
) {}