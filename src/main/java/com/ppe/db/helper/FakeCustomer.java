package com.ppe.db.helper;


import lombok.Data;
import lombok.ToString;
import software.amazon.awssdk.enhanced.dynamodb.extensions.annotations.DynamoDbVersionAttribute;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.*;

import java.util.List;

@Data
@ToString
@Table(name = "ACCOUNT")
@DynamoDbBean
public class FakeCustomer {

    // Department
    @PartitionKey
    private String PARTITION_KEY;

    @SortKey
    private String SORT_KEY;


    private String ID;
    private String CUSTOMER_NAME;

    public Integer getFees() {
        return fees;
    }

    public void setFees(Integer fees) {
        this.fees = fees;
    }

    Integer fees;

    private List<String> names;
    Integer version;
    private FakePicture fakePicture;

    private Integer junk;


    @DynamoDbVersionAttribute
    public Integer getVersion() {
        return version;
    }


    @DynamoDbPartitionKey
    public String getPARTITION_KEY() {
        return PARTITION_KEY;
    }

    @DynamoDbSortKey
    public String getSORT_KEY() {
        return SORT_KEY;
    }

    @DynamoDbAttribute("Pictures")
    public FakePicture getFakePicture() { return fakePicture;}
    public void setFakePicture(FakePicture fakePicture) {this.fakePicture = fakePicture;}

    @DynamoDbIgnore
    public Integer getJunk() { return junk; }
    public void setJunk(Integer junk) { this.junk = junk; }


}


