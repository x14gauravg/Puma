package com.ppe.processor;

import lombok.Data;

@Data
public class PPEvent {
    private Integer version;
    private String planClass;
    private Integer accountNum;

    // data specific to the event - could be transaction, payment
    private byte[] data;
}
