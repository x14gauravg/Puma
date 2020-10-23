package com.ppe.dynamomodel;

import lombok.Data;

@Data
public class ChargeEntry {

    private Integer PARTITION_KEY;
    private Integer SORT_KEY;

    private Integer value;
    private String DR_CR;
}
