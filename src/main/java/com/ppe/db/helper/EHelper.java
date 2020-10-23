package com.ppe.db.helper;

import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.TransactionCanceledException;

import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Function;

public class EHelper {

    private static DynamoDbEnhancedClient getEnhancedClient() {
        Region region = Region.US_EAST_1;
        DynamoDbClient ddb = DynamoDbClient.builder()
                .region(region)
                .build();

        DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(ddb)
                .build();

        return enhancedClient;
    }

    public static <T> T getItem(T obj) {
        final T t = queryTable(obj, null, false, k -> QueryConditional.keyEqualTo(k)).get(0);
        return (t != null) ? t : null;
    }

    public static <T> List<T> querySortKeyBeginsWith(T obj, String filterExpression, boolean scanIndexForward) {
        return queryTable(obj, filterExpression, scanIndexForward, k -> QueryConditional.sortBeginsWith(k));
    }

    public static <T> List<T> querySortKeyGreaterThan(T obj, String filterExpression, boolean scanIndexForward) {
        return queryTable(obj, filterExpression, scanIndexForward, k -> QueryConditional.sortGreaterThan(k));
    }

    public static <T> List<T> querySortKeyLessThan(T obj, String filterExpression, boolean scanIndexForward) {
        return queryTable(obj, filterExpression, scanIndexForward, k -> QueryConditional.sortLessThan(k));
    }

    public static <T> List<T> querySortKeyGreaterThanOrEqualTo(T obj, String filterExpression, boolean scanIndexForward) {
        return queryTable(obj, filterExpression, scanIndexForward, k -> QueryConditional.sortGreaterThanOrEqualTo(k));
    }

    public static <T> List<T> querySortKeyLessThanOrEqualTo(T obj, String filterExpression, boolean scanIndexForward) {
        return queryTable(obj, filterExpression, scanIndexForward, k -> QueryConditional.sortLessThanOrEqualTo(k));
    }

    private static <T> List<T> queryTable(T obj, String filterExpression, boolean scanIndexForward, Function<Key, QueryConditional> queryFunction) {

        List<T> result = new ArrayList<>();
        List<Field> fields = getFieldsInClass(obj);
        final Field[] declaredFields = getFieldsInClass(obj).toArray(new Field[fields.size()]);

        final List<String> filterExpressionList = getTokenFromExpression(filterExpression);

        // Create a KEY object
        Key k = getPrimaryKeyForTable(obj);

        DynamoDbTable<T> queryTable = (DynamoDbTable<T>) getEnhancedClient().table(getDynamoDBTableName(obj), TableSchema.fromClass(obj.getClass()));

        final Map<String, AttributeValue> stringAttributeValueMap = queryTable.tableSchema().itemToMap(obj, filterExpressionList);
        Map<String, AttributeValue> expressionValueMap = new HashMap<>();
        stringAttributeValueMap.forEach((s, av) -> expressionValueMap.put(":" + s, av));


        Expression expression = Expression.builder()
                .expression(filterExpression)
                .expressionValues(expressionValueMap)
                .build();

        QueryEnhancedRequest queryRequest = QueryEnhancedRequest.builder()
                .queryConditional(queryFunction.apply(k))
                .filterExpression(expression)
                .build();

        final PageIterable<T> pageIterable = queryTable.query(queryRequest);
        pageIterable.stream().forEach(p -> p.items().forEach(item -> result.add(item)));
        return result;
    }


    public static <T> void putItem(T obj, Class<T> clazz) {

        final String keyExpression = primaryKeyCheckExpression(obj, true, false);
        final List<String> expressionList = getTokenFromExpression(keyExpression);

        final DynamoDbTable<T> table = (DynamoDbTable<T>) getEnhancedClient().table(getDynamoDBTableName(obj), TableSchema.fromClass(obj.getClass()));

        final Map<String, AttributeValue> stringAttributeValueMap = table.tableSchema().itemToMap(obj, expressionList);
        Map<String, AttributeValue> expressionValueMap = new HashMap<>();
        stringAttributeValueMap.forEach((s, av) -> expressionValueMap.put(":" + s, av));

        Expression expression = Expression.builder()
                .expression(keyExpression)
                .expressionValues(expressionValueMap)
                .build();

        PutItemEnhancedRequest<T> put = PutItemEnhancedRequest.builder(clazz)
                .conditionExpression(expression)
                .item(obj)
                .build();

        table.putItem(put);

    }

    private static <T> String getDynamoDBTableName(T obj) {
        Table annotation = null;
        if (obj.getClass().isAnnotationPresent(Table.class)) {
            annotation = obj.getClass().getAnnotation(Table.class);
        }
        return annotation != null ? annotation.name() : null;
    }

    private static <T> String getDynamoDBTableName(Class<T> clazz) {
        Table annotation = null;
        if (clazz.isAnnotationPresent(Table.class)) {
            annotation = clazz.getAnnotation(Table.class);
        }
        return annotation != null ? annotation.name() : null;
    }


    private static <T> List<Field> getFieldsInClass(T t) {
        List<Field> fields = new ArrayList<>();
        Class clazz = t.getClass();
        while (clazz != Object.class) {
            fields.addAll(Arrays.asList(clazz.getDeclaredFields()));
            clazz = clazz.getSuperclass();
        }
        return fields;
    }

    private static List<String> getTokenFromExpression(String conditionExpression) {
        StringBuffer buf = new StringBuffer();
        List<String> l = new ArrayList<>();
        if (null == conditionExpression) return l;

        String[] splited = conditionExpression.split("\\s+");
        for (String s : splited) {
            if (s.startsWith(":")) {
                l.add(s.substring(1));
            }
        }
        return l;
    }


    private static <T> Key getPrimaryKeyForTable(T obj) {
        // Create a KEY object
        Key.Builder keyBuilder = Key.builder();
        List<Field> fields = getFieldsInClass(obj);
        final Field[] declaredFields = getFieldsInClass(obj).toArray(new Field[fields.size()]);

        for (Field f : declaredFields) {
            Object value = null;
            try {
                f.setAccessible(true);
                value = f.get(obj);
                AttributeValue av = null;

                if (null != value) {

                    if (f.isAnnotationPresent(PartitionKey.class) && (f.getType() == Integer.class)) {
                        keyBuilder.partitionValue((Integer) value);
                        continue;
                    }
                    if (f.isAnnotationPresent(PartitionKey.class) && (f.getType() == String.class)) {
                        keyBuilder.partitionValue((String) value);
                        continue;
                    }
                    if (f.isAnnotationPresent(SortKey.class) && (f.getType() == Integer.class)) {
                        keyBuilder.sortValue((Integer) value);
                        continue;
                    }
                    if (f.isAnnotationPresent(SortKey.class) && (f.getType() == String.class)) {
                        keyBuilder.sortValue((String) value);
                        continue;
                    }

                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return keyBuilder.build();
    }



    private static <T> void putRequest(TransactWriteItemsEnhancedRequest.Builder builder, T obj, Class<T> clazz) {

        final String keyExpression = primaryKeyCheckExpression(obj, true, false);
        final List<String> expressionList = getTokenFromExpression(keyExpression);

        final DynamoDbTable<T> table = (DynamoDbTable<T>) getEnhancedClient().table(getDynamoDBTableName(obj), TableSchema.fromClass(obj.getClass()));

        final Map<String, AttributeValue> stringAttributeValueMap = table.tableSchema().itemToMap(obj, expressionList);
        Map<String, AttributeValue> expressionValueMap = new HashMap<>();
        stringAttributeValueMap.forEach((s, av) -> expressionValueMap.put(":" + s, av));

        Expression expression = Expression.builder()
                .expression(keyExpression)
                .expressionValues(expressionValueMap)
                .build();

        PutItemEnhancedRequest<T> put = PutItemEnhancedRequest.builder(clazz)
                .conditionExpression(expression)
                .item(obj)
                .build();

        builder.addPutItem(table, put);

    }

    private static <T> void updateRequest(TransactWriteItemsEnhancedRequest.Builder builder, T obj, Class<T> clazz, String conditionExpression) {

        final DynamoDbTable<T> table = (DynamoDbTable<T>) getEnhancedClient().table(getDynamoDBTableName(obj), TableSchema.fromClass(obj.getClass()));

        final List<String> expressionList = getTokenFromExpression(conditionExpression);
        final Map<String, AttributeValue> stringAttributeValueMap = table.tableSchema().itemToMap(obj, expressionList);
        Map<String, AttributeValue> expressionValueMap = new HashMap<>();
        stringAttributeValueMap.forEach((s, av) -> expressionValueMap.put(":" + s, av));

        Expression expression = Expression.builder()
                .expression(conditionExpression)
                .expressionValues(expressionValueMap)
                .build();

//        Expression expression = Expression.builder()
//                .expression("#key = :value")
//                .putExpressionName("#key", "fees")
//                .putExpressionValue(":value", AttributeValue.builder().n(((Integer) 100).toString()).build())
//                .build();

        UpdateItemEnhancedRequest<T> update = UpdateItemEnhancedRequest.builder(clazz)
                .ignoreNulls(true)
                .item(obj)
                .conditionExpression(expression)
                .build();

        builder.addUpdateItem(table, update);

    }


    private static <T> String primaryKeyCheckExpression(T obj, boolean primaryKeyCheck, boolean versionCheck) {
        String keyExpression = "";
        List<Field> fields = getFieldsInClass(obj);
        final Field[] declaredFields = getFieldsInClass(obj).toArray(new Field[fields.size()]);


        for (Field f : declaredFields) {
            Object value = null;
            try {
                f.setAccessible(true);
                value = f.get(obj);
                software.amazon.awssdk.services.dynamodb.model.AttributeValue av = null;

                if (null != value) {

                    if (f.isAnnotationPresent(PartitionKey.class)) {

                        if (!keyExpression.equalsIgnoreCase("")) keyExpression += " AND ";
                        if (primaryKeyCheck) keyExpression += f.getName() + " <> :" + f.getName() + " ";
                        continue;
                    }

                    if (f.isAnnotationPresent(SortKey.class)) {
                        if (!keyExpression.equalsIgnoreCase("")) keyExpression += " AND ";
                        if (primaryKeyCheck) keyExpression += f.getName() + " <> :" + f.getName() + " ";
                        continue;
                    }
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        return keyExpression.equalsIgnoreCase("") ? null : keyExpression;
    }

    public static class TxnPacket {

        TransactWriteItemsEnhancedRequest.Builder builder = TransactWriteItemsEnhancedRequest.builder();

        public static TxnPacketBuilder builder() {
            return new TxnPacketBuilder(new TxnPacket());
        }

        public static class TxnPacketBuilder {

            private final TxnPacket tp;

            TxnPacketBuilder(TxnPacket tp) {
                this.tp = tp;
            }

            public <T> TxnPacketBuilder update(T updateObject, Class<T> clazz, String conditionExpression) {
                updateRequest(tp.builder, updateObject, clazz, conditionExpression);
                return this;
            }

            public <T> TxnPacketBuilder insert(T insertObject, Class<T> clazz) {
                putRequest(tp.builder, insertObject, clazz);
                return this;
            }

            public TxnPacket build() {
                return tp;
            }
        }
    }

    public static void executeTransactionWrite(TxnPacket tp) {
            getEnhancedClient().transactWriteItems(tp.builder.build());
    }
}
