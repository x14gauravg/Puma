package com.ppe.processor;

import com.ppe.dynamomodel.AccountBalanceItem;
import com.ppe.dynamomodel.AccountConfiguration;
import lombok.Data;

@Data
public class AccountContext {

    private AccountConfiguration accountConfiguration;
    private AccountBalanceItem accountBalanceItem;


}
