package dk.ku.di.dms.vms.tpcc.replica.repositories;

import dk.ku.di.dms.vms.modb.api.annotations.Query;
import dk.ku.di.dms.vms.modb.api.annotations.Repository;
import dk.ku.di.dms.vms.modb.api.interfaces.IRepository;
import dk.ku.di.dms.vms.tpcc.replica.dto.DistrictInfoDTO;
import dk.ku.di.dms.vms.tpcc.replica.entities.District;

@Repository
public interface IDistrictRepository extends IRepository<District.DistrictId, District> {

    @Query("select d_next_o_id, d_tax from district where d_w_id = :d_w_id and d_id = :d_id")
    DistrictInfoDTO getNextOidAndTax(int d_w_id, int d_id);

}