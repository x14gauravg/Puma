package com.ppe.dynamomodel;

import com.ppe.db.helper.PartitionKey;
import com.ppe.db.helper.SortKey;
import com.ppe.db.helper.Table;
import com.ppe.db.trial.IgnoreOnInsert;
import com.ppe.db.trial.IgnoreOnUpdate;
import com.ppe.db.trial.IncrementOnUpdate;
import lombok.Data;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

@Data
@Table(name = "ACCOUNT")
@DynamoDbBean
public class Account  {

    @DynamoDbPartitionKey
    public String getPARTITION_KEY() {
        return PARTITION_KEY;
    }

    @DynamoDbSortKey
    public String getSORT_KEY() {
        return SORT_KEY;
    }

    @PartitionKey
    private String PARTITION_KEY;

    @SortKey
    private String SORT_KEY;

    Integer accountNum;

    @IncrementOnUpdate
    Integer amount;

    @IgnoreOnInsert
    @IgnoreOnUpdate
    Integer junk;

    @IncrementOnUpdate
    Integer newVal;

    @IncrementOnUpdate
    Integer oneMore;

    @IncrementOnUpdate
    Integer testAgain;

    @IncrementOnUpdate
    Integer newInt;

    String newString;




}
