package com.ppe.processor.retailposting;

import com.ppe.dynamomodel.AccountConfiguration;
import com.ppe.dynamomodel.ChargeEntry;
import com.ppe.dynamomodel.RetailPlanBalanceItem;
import com.ppe.dynamomodel.RetailPlanConfiguration;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;


public enum EnumRetailPlan {
    PRINCIPAL("charge" , Integer.class ), INTEREST("charge", Integer.class)
    , FEES("charge", Integer.class) , CURRENT_BALANCE("calculated" , Integer.class);

    private String bucketType;
    private Class typeClass;

    EnumRetailPlan(String bucketType, Class clazz) { this.bucketType = bucketType;  this.typeClass = clazz;}


    public void setValueInRetailPlanBalaneItem(RetailPlanBalanceItem retailPlanBalanceItem, ChargeEntry ce) throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        Method m = retailPlanBalanceItem.getClass().getDeclaredMethod("set" + this.name(), this.typeClass);
        m.setAccessible(true);
        m.invoke(retailPlanBalanceItem,ce.getValue());
    }

    // utility function to get charge entry created
    public ChargeEntry getChargeEntry(int value, AccountConfiguration accountConfiguration, RetailPlanConfiguration planConfiguration ) { return null; }

    // utility function to get retail plan buckets update looking at charge entries
    public static RetailPlanBalanceItem getRetailPlanBalanceItem(HashMap<EnumRetailPlan, ChargeEntry> map, AccountConfiguration accountConfiguration, RetailPlanConfiguration planConfiguration) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        RetailPlanBalanceItem rp = new RetailPlanBalanceItem();

        // set partition, sort or any other GSI keys
//        rp.setPARTITION_KEY(accountConfiguration.getAccountNum());
//        rp.setSORT_KEY(planConfiguration.getPlanId());

        for (Map.Entry<EnumRetailPlan, ChargeEntry> entry : map.entrySet()) {
            EnumRetailPlan bucket = entry.getKey();
            ChargeEntry ce = entry.getValue();
            bucket.setValueInRetailPlanBalaneItem(rp, ce);
        }
        return rp;
    }

}
