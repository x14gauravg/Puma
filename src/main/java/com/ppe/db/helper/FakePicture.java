package com.ppe.db.helper;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBAttribute;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBDocument;
import lombok.Data;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbBean;

@Data
@DynamoDBDocument
@DynamoDbBean
public class FakePicture {
    private String frontView;
    private String rearView;
    private String sideView;

    @DynamoDBAttribute(attributeName = "FrontView")
    public String getFrontView() { return frontView; }
    public void setFrontView(String frontView) { this.frontView = frontView; }

    @DynamoDBAttribute(attributeName = "RearView")
    public String getRearView() { return rearView; }
    public void setRearView(String rearView) { this.rearView = rearView; }

    @DynamoDBAttribute(attributeName = "SideView")
    public String getSideView() { return sideView; }
    public void setSideView(String sideView) { this.sideView = sideView; }

}