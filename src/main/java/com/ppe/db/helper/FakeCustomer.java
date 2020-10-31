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
    private  String PARTITION_KEY;

    private String SORT_KEY;

    private List<FakePicture> p;


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

    public FakePicture getImg() {
        return img;
    }


    public void setImg(FakePicture img) {
        this.img = img;
    }

    private FakePicture img;

    private Integer junk;

    // Update x = x + 10

    // Get  Object ,  X = 40, Version = 1
    // Update - X = 50, version = 1 only update
    //

    // Insert an item - i want it to fail if PK and SK are already there

    //


    @DynamoDbVersionAttribute
    public Integer getVersion() {
        return version;
    }


    // comma separated list
    @DynamoDbSecondarySortKey(indexNames = {"TEST_IDX"})
    @DynamoDbPartitionKey
    public String getPARTITION_KEY() {
        return PARTITION_KEY;
    }

    @DynamoDbSecondaryPartitionKey(indexNames = "TEST_IDX")
    @DynamoDbSortKey
    public String getSORT_KEY() {
        return SORT_KEY;
    }

//    @DynamoDbAttribute("Pictures")
//    public FakePicture getFakePicture() { return fakePicture;}
//    public void setFakePicture(FakePicture fakePicture) {this.fakePicture = fakePicture;}

    @DynamoDbIgnore
    public Integer getJunk() { return junk; }
    public void setJunk(Integer junk) { this.junk = junk; }


}


