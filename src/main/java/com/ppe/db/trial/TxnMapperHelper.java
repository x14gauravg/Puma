package com.ppe.db.trial;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.*;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.InternalServerErrorException;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TransactionCanceledException;
import com.ppe.db.helper.PartitionKey;
import com.ppe.db.helper.SortKey;

import java.lang.reflect.Field;
import java.util.*;

public class TxnMapperHelper {

    public static DynamoDBMapper getMapper(){

        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();

        DynamoDBMapperConfig mapperConfig = DynamoDBMapperConfig.builder()
                .withSaveBehavior(DynamoDBMapperConfig.SaveBehavior.APPEND_SET)
                .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)
                .withTableNameOverride(null)
                .withPaginationLoadingStrategy(DynamoDBMapperConfig.PaginationLoadingStrategy.EAGER_LOADING)
                .build();

        DynamoDBMapper mapper = new DynamoDBMapper(client, mapperConfig);

        return mapper;

    }


    public static class MapperPacket {

        TransactionWriteRequest transactionWriteRequest = new TransactionWriteRequest();

        public static MapperPacketBuilder builder() {
            return new MapperPacketBuilder(new MapperPacket());
        }

        public static class MapperPacketBuilder {

           private final MapperPacket tp;

           MapperPacketBuilder(MapperPacket tp) {
                this.tp = tp;
            }

            public MapperPacketBuilder update(Object updateObject, String conditionExpression) {
                DynamoDBTransactionWriteExpression expression = new DynamoDBTransactionWriteExpression();
                String keyExpression = enhanceConditionExpression(updateObject, conditionExpression, false, true);
                expression.withExpressionAttributeValues(getExpressionMap(updateObject,keyExpression));
                expression.withConditionExpression(keyExpression);
                checkNullVersion(updateObject);
                tp.transactionWriteRequest.addUpdate(updateObject, expression);
                return this;
            }



            public MapperPacketBuilder insert(Object insertObject, String conditionExpression) {
                DynamoDBTransactionWriteExpression expression = new DynamoDBTransactionWriteExpression();
                String keyExpression = enhanceConditionExpression(insertObject, conditionExpression, true, false);
                expression.withExpressionAttributeValues(getExpressionMap(insertObject,keyExpression));
                expression.withConditionExpression(keyExpression);
                tp.transactionWriteRequest.addPut(insertObject, expression);
                return this;
            }

            public MapperPacket build() {
                return tp;
            }
        }
    }

    public static void executeTransactionWrite(MapperPacket mp) {
        try {
            getMapper().transactionWrite(mp.transactionWriteRequest);
        } catch (DynamoDBMappingException ddbme) {
            System.err.println("Client side error in Mapper, fix before retrying. Error: " + ddbme.getMessage());
        } catch (ResourceNotFoundException rnfe) {
            System.err.println("One of the tables was not found, verify table exists before retrying. Error: " + rnfe.getMessage());
        } catch (InternalServerErrorException ise) {
            System.err.println("Internal Server Error, generally safe to retry with back-off. Error: " + ise.getMessage());
        } catch (TransactionCanceledException tce) {
            System.err.println("Transaction Canceled, implies a client issue, fix before retrying. Error: " + tce.getMessage());
        } catch (Exception ex) {
            System.err.println("An exception occurred, investigate and configure retry strategy. Error: " + ex.getMessage());
        }
    }



    private static <T> Map<String, AttributeValue> getExpressionMap(T obj, String conditionExpression) {
        List<Field> fields = getFields(obj);

        final Field[] declaredFields = getFields(obj).toArray(new Field[fields.size()]);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        final List<String> conditionExpressionList = buildExpressionNameList(conditionExpression);

        for (Field f : declaredFields) {
            Object value = null;
            try {
                f.setAccessible(true);
                value = f.get(obj);
                AttributeValue av = null;

                if (null != value) {

                    if (f.getType() == Integer.class) {
                        av = new AttributeValue().withN(((Integer) value).toString());
                    }
                    if (f.getType() == String.class) {
                        av = new AttributeValue().withS((String) value);
                    }

                    if (conditionExpressionList.contains(":" + f.getName())) {
                        expressionAttributeValues.put(":" + f.getName(), av);
                    }

                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        return expressionAttributeValues;

    }

    private static <T> String enhanceConditionExpression(T obj, String conditionExpression, boolean primaryKeyCheck, boolean versionCheck) {
        String additionalExpression = getTableKeyExpression(obj,primaryKeyCheck,versionCheck);
        return (conditionExpression==null) ? additionalExpression : additionalExpression + " AND " + conditionExpression;

    }


    private static <T> List<Field> getFields(T t) {
        List<Field> fields = new ArrayList<>();
        Class clazz = t.getClass();
        while (clazz != Object.class) {
            fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
            clazz = clazz.getSuperclass();
        }
        return fields;
    }

    private static List<String> buildExpressionNameList(String conditionExpression) {
        StringBuffer buf = new StringBuffer();
        List<String> l = new ArrayList<>();
        if (null == conditionExpression) return l;

        String[] splited = conditionExpression.split("\\s+");
        for (String s : splited) {
            if (s.startsWith(":")) {
                l.add(s);
            }
        }
        return l;
    }

    private static <T> String getTableKeyExpression(T obj, boolean primaryKeyCheck, boolean versionCheck) {

        String keyExpression = "";
        List<Field> fields = getFields(obj);
        final Field[] declaredFields = getFields(obj).toArray(new Field[fields.size()]);


        for (Field f : declaredFields) {
            Object value = null;
            try {
                f.setAccessible(true);
                value = f.get(obj);
                software.amazon.awssdk.services.dynamodb.model.AttributeValue av = null;

                if (null != value) {

                    if (f.isAnnotationPresent(PartitionKey.class) ) {

                        if(! keyExpression.equalsIgnoreCase("")) keyExpression += " AND ";
                        if(primaryKeyCheck) keyExpression += f.getName()+ " <> :" + f.getName() + " ";
                        continue;
                    }

                    if (f.isAnnotationPresent(SortKey.class)) {
                        if(! keyExpression.equalsIgnoreCase("")) keyExpression += " AND ";
                        if(primaryKeyCheck) keyExpression += f.getName()+ " <> :" + f.getName() + " ";
                        continue;
                    }

                    if (f.isAnnotationPresent(VersionAttribute.class)) {
                        if(!keyExpression.equalsIgnoreCase("")) keyExpression += " AND ";
                        if(versionCheck) keyExpression += f.getName()+ " = :" + f.getName() + " ";
                        continue;
                    }

                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        return keyExpression.equalsIgnoreCase("") ? null : keyExpression;
    }

    private static void checkNullVersion(Object updateObject)  {
        List<Field> fields = getFields(updateObject);
        final Field[] declaredFields = getFields(updateObject).toArray(new Field[fields.size()]);

        for (Field f : declaredFields) {
            Object value = null;
            try {
                f.setAccessible(true);
                value = f.get(updateObject);
                if(value==null && f.isAnnotationPresent(VersionAttribute.class)) {
                    throw new RuntimeException("Version number can not be null for Update");
                }
                if(value!=null && f.isAnnotationPresent(VersionAttribute.class)) {
                    throw new RuntimeException("Version number can not be null for Update");
                }

            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return;
    }


}
