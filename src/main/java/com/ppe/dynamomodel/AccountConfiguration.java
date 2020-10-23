package com.ppe.dynamomodel;

import lombok.Data;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;

@Data
@DynamoDbBean
public class AccountConfiguration {
    private Integer accountNum;
}
