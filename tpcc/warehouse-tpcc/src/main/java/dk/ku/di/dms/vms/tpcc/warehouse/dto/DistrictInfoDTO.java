package dk.ku.di.dms.vms.tpcc.warehouse.dto;

public final class DistrictInfoDTO {

    public int d_next_o_id;
    public float d_tax;

    public DistrictInfoDTO(){}

    public float d_tax() {
        return this.d_tax;
    }

    public int d_next_o_id() {
        return this.d_next_o_id;
    }

}