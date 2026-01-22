package dk.ku.di.dms.vms.web_common.olap.dto;

import java.util.List;

public record OlapScanResponse(
        String schema,
        String table,
        List<Object[]> rows
) {}