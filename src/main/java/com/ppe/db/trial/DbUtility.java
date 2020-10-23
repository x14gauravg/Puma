package com.ppe.db.trial;


import com.ppe.db.helper.PartitionKey;
import com.ppe.db.helper.SortKey;
import com.ppe.db.helper.Table;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.lang.reflect.Field;
import java.util.*;

public class DbUtility {

    public static DynamoDbEnhancedClient getEnhancedClient() {
        Region region = Region.US_EAST_1;
        DynamoDbClient ddb = DynamoDbClient.builder()
                .region(region)
                .build();

        DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(ddb)
                .build();

        return enhancedClient;

    }

    private static DynamoDbClient getClient() {

        final DynamoDbClient dynamoClient = DynamoDbClient.builder()
                .region(Region.US_EAST_1)
                .build();

        return dynamoClient;

    }


    public static class TransactionPacket {

        Map<String, ArrayList<Update>> updateMap = new HashMap<>();
        Map<String, ArrayList<Put>> insertMap = new HashMap<>();


        public static TransactionPacketBuilder builder() {
            return new TransactionPacketBuilder(new TransactionPacket());
        }

        public static class TransactionPacketBuilder {

            private final TransactionPacket tp;

            TransactionPacketBuilder(TransactionPacket tp) {
                this.tp = tp;
            }

            public TransactionPacketBuilder update(Object updateObject, String table, String conditionExpression) {
                ArrayList<Update> list = tp.updateMap.get(table);
                if (null == list) list = new ArrayList<>();
                list.add(updatePOJO(updateObject, conditionExpression));
                tp.updateMap.put(table, list);
                return this;
            }

            public TransactionPacketBuilder insert(Object insertObject, String table, String conditionExpression) {
                ArrayList<Put> list = tp.insertMap.get(table);
                if (null == list) list = new ArrayList<>();
                list.add(insertPOJO(insertObject, conditionExpression));
                tp.insertMap.put(table, list);
                return this;
            }

            public TransactionPacket build() {
                return tp;
            }
        }
    }

    public static void postTransaction(TransactionPacket tp) {

        ArrayList<TransactWriteItem> itemList = new ArrayList<>();

        tp.insertMap.forEach((table, list) -> list.forEach(action -> itemList.add(TransactWriteItem.builder().put(action).build())));
        tp.updateMap.forEach((table, list) -> list.forEach(action -> itemList.add(TransactWriteItem.builder().update(action).build())));

        TransactWriteItemsRequest transactWriteItems = TransactWriteItemsRequest.builder().transactItems(itemList).build();

        DynamoDbClient dc = getClient();
        dc.transactWriteItems(transactWriteItems);
    }


    private static <T> Update updatePOJO(T obj, String conditionExpression) {
        String table = getTableName(obj);
        List<Field> fields = getFields(obj);
        final Field[] declaredFields = getFields(obj).toArray(new Field[fields.size()]);

        HashMap<String, AttributeValue> key = new HashMap<>();

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        String setExpression = " SET ";
        String addExpression = " ADD ";
        String keyExpression = enhanceConditionExpression(obj, conditionExpression, false, true);
        final List<String> conditionExpressionList = buildExpressionNameList(keyExpression);


        for (Field f : declaredFields) {
            Object value = null;
            try {
                f.setAccessible(true);
                value = f.get(obj);
                AttributeValue av = null;

                if (null != value) {

                    if (f.getType() == Integer.class) {
                        av = AttributeValue.builder().n(((Integer) value).toString()).build();
                    }
                    if (f.getType() == String.class) {
                        av = AttributeValue.builder().s((String) value).build();
                    }

                    if (conditionExpressionList.contains(":" + f.getName())) {
                        expressionAttributeValues.put(":" + f.getName(), av);
                    }
                    if (f.isAnnotationPresent(PartitionKey.class) || f.isAnnotationPresent(SortKey.class)) {
                        key.put(f.getName(), av);
                        continue;
                    }
                    if (f.isAnnotationPresent(IgnoreOnUpdate.class)) {
                        continue;
                    }

                    if (f.isAnnotationPresent(VersionAttribute.class)) {
                        if (!addExpression.equalsIgnoreCase(" ADD ")) {
                            addExpression += " , ";
                        }
                        addExpression += " " + f.getName() + " :" + f.getName();
                        Integer newVersion =  1;
                        expressionAttributeValues.put(":" + f.getName(), AttributeValue.builder().n(newVersion.toString()).build());
                        continue;
                    }

                    if (f.isAnnotationPresent(IncrementOnUpdate.class)) {
                        if (!addExpression.equalsIgnoreCase(" ADD ")) {
                            addExpression += " , ";
                        }
                        addExpression += " " + f.getName() + " :" + f.getName();
                        expressionAttributeValues.put(":" + f.getName(), av);
                    } else {
                        if (!setExpression.equalsIgnoreCase(" SET ")) {
                            setExpression += " , ";
                        }
                        setExpression += " " + f.getName() + " = " + ":" + f.getName();
                        expressionAttributeValues.put(":" + f.getName(), av);
                    }


                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }


        System.out.println(setExpression + addExpression);

        Update u = Update.builder()
                .tableName(table)
                .key(key)
                .updateExpression((setExpression.equalsIgnoreCase(" SET ") ?  " " : setExpression) + (addExpression.equalsIgnoreCase(" ADD ") ?  " " : addExpression))
                .expressionAttributeValues(expressionAttributeValues)
                .conditionExpression(conditionExpression)
                .build();

        return u;

    }


    private static <T> Put insertPOJO(T obj, String conditionExpression) {
//        final Field[] declaredFields = obj.getClass().getFields();
        String table = getTableName(obj);

        List<Field> fields = getFields(obj);
        final Field[] declaredFields = getFields(obj).toArray(new Field[fields.size()]);

        Map<String, AttributeValue> itemMap = new HashMap<>();
        Map<String, AttributeValue> expressionValues = new HashMap<>();
        String keyExpression = enhanceConditionExpression(obj, conditionExpression, true, false);
//        final List<String> conditionExpressionList = buildExpressionNameList(":PARTITION_KEY " + " :SORT_KEY " + conditionExpression);
        final List<String> conditionExpressionList = buildExpressionNameList(keyExpression);



        for (Field f : declaredFields) {
            try {
                f.setAccessible(true);
                Object value = f.get(obj);

                if (f.isAnnotationPresent(IgnoreOnInsert.class)) {
                    continue;
                }
                if(f.isAnnotationPresent(VersionAttribute.class)){
                    itemMap.put(f.getName(), AttributeValue.builder().n(((Integer) 1).toString()).build());
                    continue;
                }

                AttributeValue av = null;

                if (null != value) {

                    if (f.getType() == Integer.class) {
                        av = AttributeValue.builder().n(((Integer) value).toString()).build();
                    }
                    if (f.getType() == String.class) {
                        av = AttributeValue.builder().s((String) value).build();
                    }
                    itemMap.put(f.getName(), av);
                    if (conditionExpressionList.contains(":" + f.getName())) {
                        expressionValues.put(":" + f.getName(), av);
                    }

                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        Put p = Put.builder()
                .item(itemMap)
                .tableName(table)
                .expressionAttributeValues(expressionValues)
                .conditionExpression(keyExpression)
//                .conditionExpression(" PARTITION_KEY <> :PARTITION_KEY AND SORT_KEY <> :SORT_KEY" + ((conditionExpression != null) ? " AND " + conditionExpression : ""))
                .build();

        return p;

    }

    private static <T> String getTableName(T obj) {

        Table annotation = null;

        if (obj.getClass().isAnnotationPresent(Table.class)) {
            annotation = obj.getClass().getAnnotation(Table.class);
        }

        return annotation != null ? annotation.name() : null;
    }

    private static <T> String getTableName(Class<T> clazz) {

        Table annotation = null;

        if (clazz.isAnnotationPresent(Table.class)) {
            annotation = clazz.getAnnotation(Table.class);
        }

        return annotation != null ? annotation.name() : null;
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

    private static <T> String enhanceConditionExpression(T obj, String conditionExpression, boolean primaryKeyCheck, boolean versionCheck) {
        String additionalExpression = getTableKeyExpression(obj,primaryKeyCheck,versionCheck);
        return (conditionExpression==null) ? additionalExpression : additionalExpression + " AND " + conditionExpression;

    }



}
