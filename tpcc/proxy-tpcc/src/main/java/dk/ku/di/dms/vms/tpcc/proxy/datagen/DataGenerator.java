package dk.ku.di.dms.vms.tpcc.proxy.datagen;

import dk.ku.di.dms.vms.tpcc.proxy.entities.*;
import dk.ku.di.dms.vms.tpcc.proxy.infra.TPCcConstants;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static dk.ku.di.dms.vms.tpcc.proxy.datagen.DataGenUtils.*;

public final class DataGenerator {

    public static Item generateItem(int I_ID) {
        int I_IM_ID = randomNumber(1, 10000);
        String I_NAME = makeAlphaString(14, 24);
        float I_PRICE = (float) ((randomNumber(100, 10000)) / 100.0);
        String I_DATA = makeAlphaString(26, 50);
        return new Item(I_ID, I_IM_ID, I_PRICE, I_NAME, I_DATA);
    }

    public static Warehouse generateWarehouse(int W_ID)
    {
        String W_NAME = makeAlphaString(6, 10);
        String W_STREET_1 = makeAlphaString(10, 20);
        String W_STREET_2 = makeAlphaString(10, 20);
        String W_CITY = makeAlphaString(10, 20);
        String W_STATE = makeAlphaString(2, 2);
        String W_ZIP = makeAlphaString(9, 9);
        float W_TAX = (float)((float) randomNumber(10, 20) / 100.0);
        float W_YTD = 3000000;
        return new Warehouse(W_ID, W_NAME, W_STREET_1, W_STREET_2, W_CITY, W_STATE, W_ZIP, W_TAX, W_YTD);
    }

    public static District generateDistrict(int D_ID, int D_W_ID)
    {
        String D_NAME = makeAlphaString(6, 10);
        String D_STREET_1 = makeAlphaString(10, 20);
        String D_STREET_2 = makeAlphaString(10, 20);
        String D_CITY = makeAlphaString(10, 20);
        String D_STATE = makeAlphaString(2, 2);
        String D_ZIP = makeAlphaString(9, 9);
        float D_TAX = (float) (((float) randomNumber(10, 20)) / 100.0);
        float D_YTD = (float) 30000.0;
        int D_NEXT_O_ID = 3001;
        return new District(D_ID, D_W_ID, D_NAME, D_STREET_1, D_STREET_2, D_CITY, D_STATE, D_ZIP, D_TAX, D_YTD, D_NEXT_O_ID);
    }

    public static Customer generateCustomer(int c_id, int c_d_id, int c_w_id) {
        String C_FIRST = makeAlphaString(8, 16);
        String C_MIDDLE = "O" + "E";
        String C_LAST;
        if (c_id <= 1000) {
            C_LAST = lastName(c_id - 1);
        } else {
            C_LAST = lastName(nuRand(255, 157, 0, 999));
        }

        String C_STREET_1 = makeAlphaString(10, 20);
        String C_STREET_2 = makeAlphaString(10, 20);
        String C_CITY = makeAlphaString(10, 20);
        String C_STATE = makeAlphaString(2, 2);
        String C_ZIP = makeAlphaString(9, 9);
        String C_PHONE = makeNumberString(16, 16);
        Date C_SINCE = new Date();

        String C_CREDIT;
        if (randomNumber(0, 1) == 1)
            C_CREDIT = "G";
        else
            C_CREDIT = "B";
        C_CREDIT += "C";

        int C_CREDIT_LIM = 50000;
        float C_DISCOUNT = (float) (((float) randomNumber(0, 50)) / 100.0);
        float C_BALANCE =  (float) -10.0;

        int C_YTD_PAYMENT = 10;
        int C_PAYMENT_CNT = 1;
        int C_DELIVERY_CNT = 0;
        String C_DATA = makeAlphaString(300, 500);

        return new Customer(c_id, c_d_id, c_w_id, C_FIRST, C_MIDDLE, C_LAST, C_STREET_1, C_STREET_2, C_CITY, C_STATE, C_ZIP,
                C_PHONE, C_SINCE, C_CREDIT, C_CREDIT_LIM, C_DISCOUNT, C_BALANCE, C_YTD_PAYMENT, C_PAYMENT_CNT, C_DELIVERY_CNT, C_DATA);
    }

    public static Stock generateStockItem(int w_id, int S_I_ID) {
        int S_QUANTITY = randomNumber(10, 100);
        Map<Integer, String> S_DIST = new HashMap<>();
        for (int d = 1; d <= TPCcConstants.NUM_DIST_PER_WARE; d++) S_DIST.put(d, makeAlphaString(24, 24));
        int S_YTD = 0;
        int S_ORDER_CNT = 0;
        int S_REMOTE_CNT = 0;
        String S_DATA = makeAlphaString(26, 50);
        return new Stock(S_I_ID, w_id, S_QUANTITY, S_DIST, S_YTD, S_ORDER_CNT, S_REMOTE_CNT, S_DATA);
    }

}