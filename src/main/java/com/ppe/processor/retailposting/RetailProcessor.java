package com.ppe.processor.retailposting;

import com.ppe.dynamomodel.AccountConfiguration;
import com.ppe.dynamomodel.ChargeEntry;
import com.ppe.dynamomodel.RetailPlanConfiguration;
import com.ppe.processor.AccountContext;
import com.ppe.processor.BaseProcessor;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.function.BiConsumer;

@Component
public class RetailProcessor extends BaseProcessor{

    @Override
    public String registerPlanClass() {
        // use ENUM
        return null;
    }

    @Override
    public BiConsumer<AccountContext, byte[]> registerHandler() {
        return this;
    }

    @Override
    public void accept(AccountContext accountContext, byte[] data) {
//        // convert to actual retail transaction using mapper and byte array
//
//        // identify Plan
//        int planId = 1;
//
//        // call for Retail Plan Configuration
//        final RetailPlanConfiguration planConfiguration = DbUtilityStandard.getPlanConfiguration(null, planId,RetailPlanConfiguration.class);
//        HashMap<EnumRetailPlan, ChargeEntry> mapOfChargeEntries = new HashMap<>();
//
//        // list of all bucket functions
//        principal(accountContext.getAccountConfiguration(),planConfiguration,mapOfChargeEntries);
//        fee(accountContext.getAccountConfiguration(),planConfiguration,mapOfChargeEntries);
//
//        // update dynamo
//        updateDynamo(mapOfChargeEntries);

    }

    private void fee(AccountConfiguration accountConfiguration, RetailPlanConfiguration planConfiguration, HashMap<EnumRetailPlan, ChargeEntry> mapOfChargeEntries) {
        ChargeEntry chargeEntry = EnumRetailPlan.FEES.getChargeEntry(20, accountConfiguration, planConfiguration);
        mapOfChargeEntries.put(EnumRetailPlan.FEES, chargeEntry);
    }

    private void principal(AccountConfiguration accountConfiguration, RetailPlanConfiguration planConfiguration, HashMap<EnumRetailPlan, ChargeEntry> mapOfChargeEntries) {
        ChargeEntry chargeEntry = EnumRetailPlan.PRINCIPAL.getChargeEntry(20, accountConfiguration, planConfiguration);
        mapOfChargeEntries.put(EnumRetailPlan.PRINCIPAL, chargeEntry);
    }

    private void updateDynamo(HashMap<EnumRetailPlan, ChargeEntry> mapOfChargeEntries) {

//        ArrayList<RetailPlanBalanceItem> updateItems = new ArrayList<>();
//        updateItems.add(EnumRetailPlan.getRetailPlanBalanceItem(mapOfChargeEntries));
//
//        ArrayList<ChargeEntry> insertItems = new ArrayList<>();
//        mapOfChargeEntries.forEach((bucket, ce) -> insertItems.add(ce));
//
//        DbUtilityStandard.transactionalWrite(insertItems,ChargeEntry.class,updateItems, RetailPlanBalanceItem.class);

    }


}
