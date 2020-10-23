package com.ppe.dynamomodel;

import com.ppe.db.helper.PartitionKey;
import com.ppe.db.helper.SortKey;
import com.ppe.db.helper.Table;
import com.ppe.db.trial.IgnoreOnInsert;
import com.ppe.db.trial.IgnoreOnUpdate;
import com.ppe.db.trial.IncrementOnUpdate;
import com.ppe.db.trial.VersionAttribute;
import lombok.Data;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;

@Table(name = "ACCOUNT")
@Data
@DynamoDbBean
public class Customer {


    // Department
    @PartitionKey
    private String PARTITION_KEY;

    // Id
    @SortKey
    private String SORT_KEY;


    private String ID;

    private String CUSTOMER_NAME;

    @IncrementOnUpdate
    Integer fees;

    @IgnoreOnUpdate
    @IgnoreOnInsert
    Integer OLDFEE;

    @VersionAttribute
    Integer version;

    @DynamoDbPartitionKey
    public String getPARTITION_KEY() {
        return PARTITION_KEY;
    }

    @DynamoDbSortKey
    public String getSORT_KEY() {
        return SORT_KEY;
    }

    public Integer getVersion() {
        return (null== version) ? 1 : version + 1;
    }

}
