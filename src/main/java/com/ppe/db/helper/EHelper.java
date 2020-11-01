package com.ppe.db.helper;

import software.amazon.awssdk.core.pagination.sync.SdkIterable;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.extensions.VersionedRecordExtension;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondaryPartitionKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSecondarySortKey;
import software.amazon.awssdk.enhanced.dynamodb.mapper.annotations.DynamoDbSortKey;
import software.amazon.awssdk.enhanced.dynamodb.model.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.beans.Introspector;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
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
                .extensions(VersionedRecordExtension.builder().build())
                .build();

        return enhancedClient;
    }

    public static <T> T getItem(T obj) {
        List<T> l = queryTable(obj, null, false, k -> QueryConditional.keyEqualTo(k));
        T t = (l.size() == 0 ) ? null : l.get(0);
        return t;
    }

    public static <T> List<T> getIndexItem(T obj, String indexName) {
        List<T> l = queryIndex(indexName, obj, null, false, k -> QueryConditional.keyEqualTo(k));
        return l;
    }

    public static <T> List<T> querySortKeyBeginsWith(T obj) {
        return querySortKeyBeginsWith(obj,null, true);
    }

    public static <T> List<T> querySortKeyBeginsWith(T obj, String filterExpression) {
        return querySortKeyBeginsWith(obj,filterExpression, true);
    }


    public static <T> List<T> querySortKeyBeginsWith(T obj, String filterExpression, boolean scanIndexForward) {
        return queryTable(obj, filterExpression, scanIndexForward, k -> QueryConditional.sortBeginsWith(k));
    }


    public static <T> List<T> querySortKeyGreaterThan(T obj, String filterExpression, boolean scanIndexForward) {
        return queryTable(obj, filterExpression, scanIndexForward, k -> QueryConditional.sortGreaterThan(k));
    }

    public static <T> List<T> querySortKeyGreaterThan(T obj) {
        return querySortKeyGreaterThan(obj,null,true);
    }

    public static <T> List<T> querySortKeyGreaterThan(T obj, String filterExpression) {
        return querySortKeyGreaterThan(obj,filterExpression,true);
    }


    public static <T> List<T> querySortKeyLessThan(T obj, String filterExpression, boolean scanIndexForward) {
        return queryTable(obj, filterExpression, scanIndexForward, k -> QueryConditional.sortLessThan(k));
    }

    public static <T> List<T> querySortKeyLessThan(T obj) {
        return querySortKeyLessThan(obj,null, true);
    }

    public static <T> List<T> querySortKeyLessThan(T obj, String filterExpression) {
        return querySortKeyLessThan(obj,filterExpression, true);
    }

    public static <T> List<T> querySortKeyGreaterThanOrEqualTo(T obj, String filterExpression, boolean scanIndexForward) {
        return queryTable(obj, filterExpression, scanIndexForward, k -> QueryConditional.sortGreaterThanOrEqualTo(k));
    }

    public static <T> List<T> querySortKeyGreaterThanOrEqualTo(T obj) {
        return querySortKeyGreaterThanOrEqualTo(obj,null,true);
    }

    public static <T> List<T> querySortKeyGreaterThanOrEqualTo(T obj, String filterExpression) {
        return querySortKeyGreaterThanOrEqualTo(obj,filterExpression,true);
    }


    public static <T> List<T> querySortKeyLessThanOrEqualTo(T obj, String filterExpression, boolean scanIndexForward) {
        return queryTable(obj, filterExpression, scanIndexForward, k -> QueryConditional.sortLessThanOrEqualTo(k));
    }

    public static <T> List<T> querySortKeyLessThanOrEqualTo(T obj) {
        return querySortKeyLessThanOrEqualTo(obj,null,true);
    }

    public static <T> List<T> querySortKeyLessThanOrEqualTo(T obj, String filterExpression) {
        return querySortKeyLessThanOrEqualTo(obj,filterExpression,true);
    }

    public static <T> List<T> queryIndexSortKeyBeginsWith(T obj, String indexName, String filterExpression, boolean scanIndexForward) {
        return queryIndex(indexName, obj, filterExpression, scanIndexForward, k -> QueryConditional.sortBeginsWith(k));
    }

    public static <T> List<T> queryIndexSortKeyBeginsWith(T obj, String indexName) {
        return queryIndexSortKeyBeginsWith(obj, indexName,null,true);
    }

    public static <T> List<T> queryIndexSortKeyBeginsWith(T obj, String indexName, String filterExpression) {
        return queryIndexSortKeyBeginsWith(obj, indexName,filterExpression,true);
    }


    public static <T> List<T> queryIndexSortKeyGreaterThan(T obj, String indexName, String filterExpression, boolean scanIndexForward) {
        return queryIndex(indexName, obj, filterExpression, scanIndexForward, k -> QueryConditional.sortGreaterThan(k));
    }

    public static <T> List<T> queryIndexSortKeyGreaterThan(T obj, String indexName) {
        return queryIndexSortKeyGreaterThan(obj, indexName,null,true);
    }

    public static <T> List<T> queryIndexSortKeyGreaterThan(T obj, String indexName, String filterExpression) {
        return queryIndexSortKeyGreaterThan(obj, indexName,filterExpression,true);
    }

    public static <T> List<T> queryIndexSortKeyLessThan(T obj, String indexName, String filterExpression, boolean scanIndexForward) {
        return queryIndex(indexName, obj, filterExpression, scanIndexForward, k -> QueryConditional.sortLessThan(k));
    }

    public static <T> List<T> queryIndexSortKeyLessThan(T obj, String indexName) {
        return queryIndexSortKeyLessThan(obj, indexName,null,true);
    }

    public static <T> List<T> queryIndexSortKeyLessThan(T obj, String indexName, String filterExpression) {
        return queryIndexSortKeyLessThan(obj, indexName,filterExpression,true);
    }


    public static <T> List<T> queryIndexSortKeyGreaterThanOrEqualTo(T obj, String indexName, String filterExpression, boolean scanIndexForward) {
        return queryIndex(indexName, obj, filterExpression, scanIndexForward, k -> QueryConditional.sortGreaterThanOrEqualTo(k));
    }

    public static <T> List<T> queryIndexSortKeyGreaterThanOrEqualTo(T obj, String indexName) {
        return queryIndexSortKeyGreaterThanOrEqualTo(obj,indexName,null, true);
    }

    public static <T> List<T> queryIndexSortKeyGreaterThanOrEqualTo(T obj, String indexName, String filterExpression) {
        return queryIndexSortKeyGreaterThanOrEqualTo(obj,indexName,filterExpression, true);
    }

    public static <T> List<T> queryIndexSortKeyLessThanOrEqualTo(T obj, String indexName, String filterExpression, boolean scanIndexForward) {
        return queryIndex(indexName, obj, filterExpression, scanIndexForward, k -> QueryConditional.sortLessThanOrEqualTo(k));
    }

    public static <T> List<T> queryIndexSortKeyLessThanOrEqualTo(T obj, String indexName) {
        return queryIndexSortKeyLessThanOrEqualTo(obj,indexName,null,true);
    }

    public static <T> List<T> queryIndexSortKeyLessThanOrEqualTo(T obj, String indexName, String filterExpression) {
        return queryIndexSortKeyLessThanOrEqualTo(obj,indexName,filterExpression,true);
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

    private static <T> List<T> queryIndex(String indexName, T obj, String filterExpression, boolean scanIndexForward, Function<Key, QueryConditional> queryFunction) {

        List<T> result = new ArrayList<>();
        List<Field> fields = getFieldsInClass(obj);
        final Field[] declaredFields = getFieldsInClass(obj).toArray(new Field[fields.size()]);

        final List<String> filterExpressionList = getTokenFromExpression(filterExpression);

        // Create a KEY object
        Key k = getPrimaryKeyForIndex(obj, indexName);

        DynamoDbIndex<T> queryTable = (DynamoDbIndex<T>) getEnhancedClient().table(getDynamoDBTableName(obj), TableSchema.fromClass(obj.getClass())).index(indexName);

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

        final SdkIterable<Page<T>> pageIterable = queryTable.query(queryRequest);
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

    private static <T> List<Method> getMethodsInClass(T t) {
        List<Method> methods = new ArrayList<>();
        Class clazz = t.getClass();
        while (clazz != Object.class) {
            methods.addAll(Arrays.asList(clazz.getDeclaredMethods()));
            clazz = clazz.getSuperclass();
        }
        return methods;
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
        List<Method> methods = getMethodsInClass(obj);
        final Method[] declaredMethods = methods.toArray(new Method[methods.size()]);

        for (Method m : declaredMethods) {
            Object value = null;
            m.setAccessible(true);
            AttributeValue av = null;

            try {

                if (m.isAnnotationPresent(DynamoDbPartitionKey.class) && (m.getReturnType() == Integer.class)) {
                    keyBuilder.partitionValue((Integer) m.invoke(obj, null));
                    continue;
                }
                if (m.isAnnotationPresent(DynamoDbPartitionKey.class) && (m.getReturnType() == String.class)) {
                    keyBuilder.partitionValue((String) m.invoke(obj, null));
                    continue;
                }
                if (m.isAnnotationPresent(DynamoDbSortKey.class) && (m.getReturnType() == Integer.class)) {
                    Integer val = (Integer) m.invoke(obj, null);
                    if(val != null)  { keyBuilder.sortValue((Integer) m.invoke(obj, null)); }
                    continue;
                }
                if (m.isAnnotationPresent(DynamoDbSortKey.class) && (m.getReturnType() == String.class)) {
                    String val = (String) m.invoke(obj, null);
                    if(val != null)  { keyBuilder.sortValue((String) m.invoke(obj, null)); }
                    continue;
                }

            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }

        }
        return keyBuilder.build();
    }


    private static <T> Key getPrimaryKeyForIndex(T obj, String indexName) {
        // Create a KEY object
        Key.Builder keyBuilder = Key.builder();
        List<Method> methods = getMethodsInClass(obj);
        final Method[] declaredMethods = methods.toArray(new Method[methods.size()]);

        for (Method m : declaredMethods) {
            Object value = null;
            m.setAccessible(true);
            AttributeValue av = null;

            try {

                if (m.isAnnotationPresent(DynamoDbSecondaryPartitionKey.class) &&   Arrays.asList(m.getAnnotation(DynamoDbSecondaryPartitionKey.class).indexNames()).contains(indexName)  &&  (m.getReturnType() == Integer.class)) {
                    keyBuilder.partitionValue((Integer) m.invoke(obj, null));
                    continue;
                }
                if (m.isAnnotationPresent(DynamoDbSecondaryPartitionKey.class) && Arrays.asList(m.getAnnotation(DynamoDbSecondaryPartitionKey.class).indexNames()).contains(indexName)  &&  (m.getReturnType() == String.class)) {
                    keyBuilder.partitionValue((String) m.invoke(obj, null));
                    continue;
                }
                if (m.isAnnotationPresent(DynamoDbSecondarySortKey.class) && Arrays.asList(m.getAnnotation(DynamoDbSecondarySortKey.class).indexNames()).contains(indexName)  && (m.getReturnType() == Integer.class)) {
                    Integer val = (Integer) m.invoke(obj, null);
                    if(val != null)  { keyBuilder.sortValue((Integer) m.invoke(obj, null)); }
                    continue;
                }
                if (m.isAnnotationPresent(DynamoDbSecondarySortKey.class) && Arrays.asList(m.getAnnotation(DynamoDbSecondarySortKey.class).indexNames()).contains(indexName)  && (m.getReturnType() == String.class)) {
                    String val = (String) m.invoke(obj, null);
                    if(val != null)  { keyBuilder.sortValue((String) m.invoke(obj, null)); }
                    continue;
                }

            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
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
        final Map<String, AttributeValue>  stringAttributeValueMap = table.tableSchema().itemToMap(obj, expressionList);

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

        List<Method> methods = getMethodsInClass(obj);
        final Method[] declaredMethods = methods.toArray(new Method[methods.size()]);

        for (Method m : declaredMethods) {
            m.setAccessible(true);
            software.amazon.awssdk.services.dynamodb.model.AttributeValue av = null;


            if (m.isAnnotationPresent(DynamoDbPartitionKey.class)) {

                if (!keyExpression.equalsIgnoreCase("")) keyExpression += " AND ";
                // " PK <> :PK
                String partitionKey = Introspector.decapitalize(m.getName().substring(m.getName().startsWith("get")? 3 : 2));
                if (primaryKeyCheck) keyExpression += partitionKey + " <> :" + partitionKey + " ";
                continue;
            }

            if (m.isAnnotationPresent(DynamoDbSortKey.class)) {

                if (!keyExpression.equalsIgnoreCase("")) keyExpression += " AND ";

                String sortKey = Introspector.decapitalize(m.getName().substring(m.getName().startsWith("get")? 3 : 2));
                if (primaryKeyCheck) keyExpression += sortKey + " <> :" + sortKey + " ";
                continue;
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

            public <T> TxnPacketBuilder update(T updateObject, Class<T> clazz) {
                updateRequest(tp.builder, updateObject, clazz, null);
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
