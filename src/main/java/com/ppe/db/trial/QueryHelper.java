package com.ppe.db.trial;


import com.ppe.db.helper.PartitionKey;
import com.ppe.db.helper.SortKey;
import com.ppe.db.helper.Table;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Function;

public class QueryHelper {

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
        List<Field> fields = getFields(obj);
        final Field[] declaredFields = getFields(obj).toArray(new Field[fields.size()]);

        Map<String, AttributeValue> expressionValues = new HashMap<>();
        final List<String> filterExpressionList = buildExpressionNameList(filterExpression);

        // Create a KEY object
        Key k = getTableKey(obj);


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

                    if (filterExpressionList.contains(":" + f.getName())) {
                        expressionValues.put(":" + f.getName(), av);
                    }

//                    if (f.isAnnotationPresent(PartitionKey.class) && (f.getType() == Integer.class)) {
//                        keyBuilder.partitionValue((Integer) value);
//                        continue;
//                    }
//                    if (f.isAnnotationPresent(PartitionKey.class) && (f.getType() == String.class)) {
//                        keyBuilder.partitionValue((String) value);
//                        continue;
//                    }
//                    if (f.isAnnotationPresent(SortKey.class) && (f.getType() == Integer.class)) {
//                        keyBuilder.sortValue((Integer) value);
//                        continue;
//                    }
//                    if (f.isAnnotationPresent(SortKey.class) && (f.getType() == String.class)) {
//                        keyBuilder.sortValue((String) value);
//                        continue;
//                    }

                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }


        DynamoDbTable<T> queryTable = (DynamoDbTable<T>) getEnhancedClient().table(getTableName(obj), TableSchema.fromBean(obj.getClass()));
        EnhancedType<?> enhancedType = TableSchema.fromBean(obj.getClass()).itemType();


        Expression expression = Expression.builder()
                .expression(filterExpression)
                .expressionValues(expressionValues)
                .build();



        QueryEnhancedRequest queryRequest = QueryEnhancedRequest.builder()
                .queryConditional(queryFunction.apply(k))
                .filterExpression(expression)
                .build();

//        final PageIterable<T> pageIterable = queryTable.query(QueryConditional.keyEqualTo(k1 -> k1.partitionValue("JKS")));

        final PageIterable<T> pageIterable = queryTable.query(queryRequest);
        pageIterable.stream().forEach(p -> p.items().forEach(item -> result.add(item)));
        return result;
    }


    public static <T> void putItem(T obj ){
        DynamoDbTable<T> mappedTable = (DynamoDbTable<T>) getEnhancedClient().table(getTableName(obj), TableSchema.fromClass(obj.getClass()));
        System.out.println(" Table Name - " + mappedTable.tableName());
        final Map<String, AttributeValue> stringAttributeValueMap = mappedTable.tableSchema().itemToMap(obj, false);

        mappedTable.putItem(obj);


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


    private static <T> Key getTableKey(T obj) {

        // Create a KEY object
        Key.Builder keyBuilder = Key.builder();
        List<Field> fields = getFields(obj);
        final Field[] declaredFields = getFields(obj).toArray(new Field[fields.size()]);


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

    public static <T> Key getIndexKey(T obj, String index) {

        // Create a KEY object
        Key.Builder keyBuilder = Key.builder();
        List<Field> fields = getFields(obj);
        final Field[] declaredFields = getFields(obj).toArray(new Field[fields.size()]);


        for (Field f : declaredFields) {
            Object value = null;
            try {
                f.setAccessible(true);
                value = f.get(obj);

                if (null != value) {

                    if (f.isAnnotationPresent(GSPartitionKey.class)) {
                        GSPartitionKey annotation = f.getAnnotation(GSPartitionKey.class);
                        String[] ints = annotation.index();
                        Arrays.sort(ints);
                        if (Arrays.asList(ints).contains(index)) {

                            if (f.getType() == Integer.class) {
                                keyBuilder.partitionValue((Integer) value);
                                continue;
                            }
                            if (f.getType() == String.class) {
                                keyBuilder.partitionValue((String) value);
                                continue;
                            }

                        }
                    }

                    if (f.isAnnotationPresent(GSSortKey.class)) {
                        GSSortKey sortAnnotation = f.getAnnotation(GSSortKey.class);
                        String[] ints = sortAnnotation.index();

                        if (Arrays.asList(ints).contains(index)) {
                            if (f.getType() == Integer.class) {
                                keyBuilder.sortValue((Integer) value);
                                continue;
                            }
                            if (f.getType() == String.class) {
                                keyBuilder.sortValue((String) value);
                                continue;
                            }
                        }

                    }

                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }

        }
        return keyBuilder.build();

    }
}
