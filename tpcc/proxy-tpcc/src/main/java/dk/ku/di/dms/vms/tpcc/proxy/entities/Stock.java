package dk.ku.di.dms.vms.tpcc.proxy.entities;

import dk.ku.di.dms.vms.modb.api.annotations.VmsTable;
import dk.ku.di.dms.vms.modb.api.interfaces.IEntity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.validation.constraints.PositiveOrZero;
import java.io.Serializable;
import java.util.Map;

@Entity
@VmsTable(name="stock")
@IdClass(Stock.StockId.class)
public final class Stock implements IEntity<Stock.StockId> {

    public static class StockId implements Serializable {
        public int s_i_id;
        public int s_w_id;

        public StockId(int s_i_id, int s_w_id) {
            this.s_i_id = s_i_id;
            this.s_w_id = s_w_id;
        }
    }

    @Id
    public int s_i_id;

    @Id
    public int s_w_id;

    @Column
    @PositiveOrZero
    public int s_quantity;

    @Column
    public String s_dist_01;
    @Column
    public String s_dist_02;
    @Column
    public String s_dist_03;
    @Column
    public String s_dist_04;
    @Column
    public String s_dist_05;
    @Column
    public String s_dist_06;
    @Column
    public String s_dist_07;
    @Column
    public String s_dist_08;
    @Column
    public String s_dist_09;
    @Column
    public String s_dist_10;

    @Column
    public String s_data;

    public Stock(){}

    public Stock(int s_i_id, int s_w_id, int s_quantity, Map<Integer, String> s_dist, int S_YTD, int S_ORDER_CNT, int S_REMOTE_CNT, String s_data) {
        this.s_i_id = s_i_id;
        this.s_w_id = s_w_id;
        this.s_quantity = s_quantity;

        // dist
        this.s_dist_01 = s_dist.get(1);
        this.s_dist_02 = s_dist.get(2);
        this.s_dist_03 = s_dist.get(3);
        this.s_dist_04 = s_dist.get(4);
        this.s_dist_05 = s_dist.get(5);
        this.s_dist_06 = s_dist.get(6);
        this.s_dist_07 = s_dist.get(7);
        this.s_dist_08 = s_dist.get(8);
        this.s_dist_09 = s_dist.get(9);
        this.s_dist_10 = s_dist.get(10);

        this.s_data = s_data;
    }

    @Override
    public String toString() {
        return "{"
                + "\"s_i_id\":\"" + s_i_id + "\""
                + ",\"s_w_id\":\"" + s_w_id + "\""
                + ",\"s_quantity\":\"" + s_quantity + "\""
                + ",\"s_dist_01\":\"" + s_dist_01 + "\""
                + ",\"s_dist_02\":\"" + s_dist_02 + "\""
                + ",\"s_dist_03\":\"" + s_dist_03 + "\""
                + ",\"s_dist_04\":\"" + s_dist_04 + "\""
                + ",\"s_dist_05\":\"" + s_dist_05 + "\""
                + ",\"s_dist_06\":\"" + s_dist_06 + "\""
                + ",\"s_dist_07\":\"" + s_dist_07 + "\""
                + ",\"s_dist_08\":\"" + s_dist_08 + "\""
                + ",\"s_dist_09\":\"" + s_dist_09 + "\""
                + ",\"s_dist_10\":\"" + s_dist_10 + "\""
                + ",\"s_data\":\"" + s_data + "\""
                + "}";
    }
}