package com.ppe.db.trial;


import com.ppe.db.helper.PartitionKey;
import com.ppe.db.helper.SortKey;
import com.ppe.db.helper.Table;
import com.ppe.dynamomodel.AccountConfiguration;
import com.ppe.processor.PPEvent;
import software.amazon.awssdk.enhanced.dynamodb.*;
import software.amazon.awssdk.enhanced.dynamodb.model.PageIterable;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryConditional;
import software.amazon.awssdk.enhanced.dynamodb.model.QueryEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.TransactWriteItemsEnhancedRequest;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.function.Function;

public class DbUtilityBackUp {

    public static DynamoDbEnhancedClient getEnhancedClient(){
        Region region = Region.US_EAST_1;
        DynamoDbClient ddb = DynamoDbClient.builder()
                .region(region)
                .build();

        DynamoDbEnhancedClient enhancedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(ddb)
                .build();

        return enhancedClient;

    }

    private static DynamoDbClient getClient(){

        final DynamoDbClient dynamoClient = DynamoDbClient.builder()
                .region(Region.US_EAST_1)
                .build();

        return dynamoClient;

    }


    public static class TransactionPacket{

        Map<String, ArrayList<Update>> updateMap = new HashMap<>();
        Map<String, ArrayList<Put>> insertMap = new HashMap<>();


        public static TransactionPacketBuilder builder(){ return new TransactionPacketBuilder(new TransactionPacket());}

        public static class TransactionPacketBuilder {

            private final TransactionPacket tp;

            TransactionPacketBuilder(TransactionPacket tp) { this.tp = tp; }

            public TransactionPacketBuilder update(Object updateObject, String table, String conditionExpression) {
                ArrayList<Update> list = tp.updateMap.get(table);
                if(null == list) list = new ArrayList<>();
                list.add(updatePOJO(updateObject, conditionExpression));
                tp.updateMap.put(table, list);
                return this;
            }

            public TransactionPacketBuilder insert(Object insertObject, String table, String conditionExpression ) {
                ArrayList<Put> list = tp.insertMap.get(table);
                if(null == list) list = new ArrayList<>();
                list.add(insertPOJO(insertObject, conditionExpression));
                tp.insertMap.put(table, list);
                return this;
            }

            public TransactionPacket build() { return tp; }
        }
    }

    public static void postTransaction(TransactionPacket tp) {

        ArrayList<TransactWriteItem> itemList = new ArrayList<>();

        tp.insertMap.forEach((table, list) -> list.forEach( action -> itemList.add(TransactWriteItem.builder().put(action).build())));
        tp.updateMap.forEach((table, list) -> list.forEach( action -> itemList.add(TransactWriteItem.builder().update(action).build())));

        TransactWriteItemsRequest transactWriteItems = TransactWriteItemsRequest.builder().transactItems(itemList).build();

        DynamoDbClient dc = getClient();
        dc.transactWriteItems(transactWriteItems);
    }


    private static <T> Update updatePOJO(T obj ,  String conditionExpression){
        String table = getTableName(obj);
        List<Field> fields = getFields(obj);
        final Field[] declaredFields = getFields(obj).toArray(new Field[fields.size()]);

        HashMap<String, AttributeValue> key = new HashMap<>();

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        String setExpression = " SET ";
        String addExpression = " ADD ";
        final List<String> conditionExpressionList = buildExpressionNameList(conditionExpression);

        for (Field f : declaredFields){
            Object value = null;
            try {
                f.setAccessible(true);
                value = f.get(obj);
                AttributeValue av = null;

                if (null != value) {

                    if (f.getType() == Integer.class) { av = AttributeValue.builder().n(((Integer) value).toString()).build(); }
                    if (f.getType() == String.class) { av = AttributeValue.builder().s((String) value).build(); }

                    if(conditionExpressionList.contains(":"+f.getName())) {expressionAttributeValues.put(":"+f.getName(),av); }
                    if (f.isAnnotationPresent(PartitionKey.class) || f.isAnnotationPresent(SortKey.class)) { key.put(f.getName(), av); continue; }
                    if(f.isAnnotationPresent(IgnoreOnUpdate.class)) { continue; }

                    if (f.isAnnotationPresent(IncrementOnUpdate.class)) {
                        if(!addExpression.equalsIgnoreCase(" ADD ")) {addExpression += " , "; }
                        addExpression += " " + f.getName() +  " :" + f.getName();
                        expressionAttributeValues.put(":" + f.getName(), av);
                    } else {
                        if(!setExpression.equalsIgnoreCase(" SET ")) {setExpression += " , "; }
                        setExpression += " " + f.getName() + " = " + ":" + f.getName();
                        expressionAttributeValues.put(":" + f.getName(), av);
                    }
                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }


        System.out.println(setExpression+addExpression);

        Update u = Update.builder()
                .tableName(table)
                .key(key)
                .updateExpression(setExpression+addExpression)
                .expressionAttributeValues(expressionAttributeValues)
                .conditionExpression(conditionExpression)
                .build();

        return u;

    }


    private static <T> Put insertPOJO(T obj , String conditionExpression ){
//        final Field[] declaredFields = obj.getClass().getFields();
        String table = getTableName(obj);

        List<Field> fields = getFields(obj);
        final Field[] declaredFields = getFields(obj).toArray(new Field[fields.size()]);

        Map<String, AttributeValue> itemMap = new HashMap<>();
        Map<String, AttributeValue> expressionValues = new HashMap<>();
        final List<String> conditionExpressionList = buildExpressionNameList(":PARTITION_KEY " + " :SORT_KEY " + conditionExpression);


        for (Field f : declaredFields){
            try {
                f.setAccessible(true);
                Object value = f.get(obj);

                if(f.isAnnotationPresent(IgnoreOnInsert.class)) { continue; }

                AttributeValue av = null;

                if(null != value){

                    if (f.getType() == Integer.class) { av = AttributeValue.builder().n(((Integer) value).toString()).build(); }
                    if (f.getType() == String.class) { av = AttributeValue.builder().s((String) value).build(); }
                    itemMap.put(f.getName(), av);
                    if(conditionExpressionList.contains(":"+f.getName())) {expressionValues.put(":"+f.getName(),av); }

                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        Put p = Put.builder()
                .item(itemMap)
                .tableName(table)
                .expressionAttributeValues(expressionValues)
                .conditionExpression(" PARTITION_KEY <> :PARTITION_KEY AND SORT_KEY <> :SORT_KEY" + ((conditionExpression!=null) ? " AND " + conditionExpression : ""))
                .build();

        return p;

    }

    private static <T> String getTableName(T obj) {

        Table annotation = null;

        if( obj.getClass().isAnnotationPresent(Table.class)){
            annotation = obj.getClass().getAnnotation(Table.class);
        }

        return annotation!=null ? annotation.name() : null;
    }

    private static <T> String getTableName(Class<T> clazz) {

        Table annotation = null;

        if (clazz.isAnnotationPresent(Table.class)) {
            annotation = clazz.getAnnotation(Table.class);
        }

        return annotation != null ? annotation.name() : null;
    }


    public void invokeGetter(Object obj, String variableName)
    {
        try {
            PropertyDescriptor pd = new PropertyDescriptor(variableName, obj.getClass());
            Method getter = pd.getReadMethod();
            Object f = getter.invoke(obj);
            System.out.println(f);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | IntrospectionException e) {
            e.printStackTrace();
        }
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
        if(null == conditionExpression) return l;

        String[] splited = conditionExpression.split("\\s+");
        for(String s : splited) {
            if( s.startsWith(":") ) {
                l.add(s);
            }
        }
        return l;
    }


    public static <T> T getPlanConfiguration(AccountConfiguration accountConfiguration, int planId, Class<T> clazz) {
        return getItem(accountConfiguration.getAccountNum(),planId,clazz);
    }

    public static <T> T getAccountConfiguration(PPEvent ppEvent, Class<T> clazz) {
        return getItem(ppEvent.getAccountNum(),"",clazz);
    }

//    public static void postChargeEntries(List<ChargeEntry> l){
//        transactionalWrite(l,ChargeEntry.class,null,ChargeEntry.class);
//    }



//    public static <T,R> void transactionalWrite(List<T> insertItem , Class<T> insertClass, List<R> updateItem, Class<R> updateClass){
//        DynamoDbEnhancedClient enhancedClient = getEnhancedClient();
//
//        DynamoDbTable<T> insertTable = enhancedClient.table("Billing", TableSchema.fromBean(insertClass));
//        DynamoDbTable<R> updateTable = enhancedClient.table("Billing", TableSchema.fromBean(updateClass));
//
//        TransactWriteItemsEnhancedRequest.Builder builder = TransactWriteItemsEnhancedRequest.builder();
//        insertItem.forEach(t -> builder.addPutItem(insertTable,t));
//        updateItem.forEach(r -> { builder.addUpdateItem(updateTable, UpdateItemEnhancedRequest.builder(updateClass).ignoreNulls(true).item(r).build()); });
//
//        enhancedClient.transactWriteItems(builder.build());
//
//    }

    public static <T,R> void transactionalWrite(List<T> insertItem , Class<T> insertClass){
        DynamoDbEnhancedClient enhancedClient = getEnhancedClient();

        DynamoDbTable<T> insertTable = enhancedClient.table("Billing", TableSchema.fromBean(insertClass));

        TransactWriteItemsEnhancedRequest.Builder builder = TransactWriteItemsEnhancedRequest.builder();
        insertItem.forEach(t -> builder.addPutItem(insertTable,t));

        enhancedClient.transactWriteItems(builder.build());

    }

    public static <T> T getItem(String partitionKey, String sortKey, Class<T> clazz) {
        String table = getTableName(clazz);

        try {
            // Create a DynamoDbTable object
            DynamoDbTable<T> mappedTable = getEnhancedClient().table(table, TableSchema.fromBean(clazz));
            mappedTable.tableSchema().attributeNames();

            // Create a KEY object
            Key key = Key.builder()
                    .partitionValue(partitionKey)
                    .sortValue(sortKey)
                    .build();

            // Get the item by using the key
            T result = mappedTable.getItem(r -> r.key(key));
            return result;

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
            return null;
        }
    }

    public static <T> T getItem(String partitionKey, Integer sortKey, Class<T> clazz) {
        String table = getTableName(clazz);

        try {
            // Create a DynamoDbTable object
            DynamoDbTable<T> mappedTable = getEnhancedClient().table(table, TableSchema.fromBean(clazz));

            // Create a KEY object
            Key key = Key.builder()
                    .partitionValue(partitionKey)
                    .sortValue(sortKey)
                    .build();

            // Get the item by using the key
            T result = mappedTable.getItem(r -> r.key(key));
            return result;

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
            return null;
        }
    }


    public static <T> T getItem(Integer partitionKey, Integer sortKey, Class<T> clazz) {
        String table = getTableName(clazz);
        try {
            // Create a DynamoDbTable object
            DynamoDbTable<T> mappedTable = getEnhancedClient().table(table, TableSchema.fromBean(clazz));

            // Create a KEY object
            Key key = Key.builder()
                    .partitionValue(partitionKey)
                    .sortValue(sortKey)
                    .build();

            // Get the item by using the key
            T result = mappedTable.getItem(r -> r.key(key));
            return result;

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
            return null;
        }
    }

    public static <T> T getItem(Integer partitionKey, String sortKey, Class<T> clazz) {
        String table = getTableName(clazz);
        try {
            // Create a DynamoDbTable object
            DynamoDbTable<T> mappedTable = getEnhancedClient().table(table, TableSchema.fromBean(clazz));

            // Create a KEY object
            Key key = Key.builder()
                    .partitionValue(partitionKey)
                    .sortValue(sortKey)
                    .build();

            // Get the item by using the key
            T result = mappedTable.getItem(r -> r.key(key));
            return result;

        } catch (DynamoDbException e) {
            System.err.println(e.getMessage());
            System.exit(1);
            return null;
        }
    }

    private static <T> List<T> queryTable(T obj, String filterExpression, boolean scanIndexForward, Function<Key,QueryConditional> queryFunction){

        List<T> result = new ArrayList<>();
        List<Field> fields = getFields(obj);
        final Field[] declaredFields = getFields(obj).toArray(new Field[fields.size()]);

        Map<String, AttributeValue> expressionValues = new HashMap<>();
        final List<String> filterExpressionList = buildExpressionNameList(filterExpression);

        // Create a KEY object
        Key.Builder keyBuilder = Key.builder();


        for (Field f : declaredFields){
            Object value = null;
            try {
                f.setAccessible(true);
                value = f.get(obj);
                AttributeValue av = null;

                if (null != value) {

                    if (f.getType() == Integer.class) { av = AttributeValue.builder().n(((Integer) value).toString()).build(); }
                    if (f.getType() == String.class) { av = AttributeValue.builder().s((String) value).build(); }

                    if(filterExpressionList.contains(":"+f.getName())) {expressionValues.put(":"+f.getName(),av); }

                    if (f.isAnnotationPresent(PartitionKey.class) && (f.getType() == Integer.class)) { keyBuilder.partitionValue((Integer) value); continue; }
                    if (f.isAnnotationPresent(PartitionKey.class) && (f.getType() == String.class)) { keyBuilder.partitionValue((String) value); continue; }
                    if (f.isAnnotationPresent(SortKey.class) && (f.getType() == Integer.class)) { keyBuilder.sortValue((Integer) value); continue; }
                    if (f.isAnnotationPresent(SortKey.class) && (f.getType() == String.class)) { keyBuilder.sortValue((String) value); continue; }

                }
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }


        DynamoDbTable<T> queryTable = (DynamoDbTable<T>) getEnhancedClient().table(getTableName(obj), TableSchema.fromBean(obj.getClass()));

        Expression expression = Expression.builder()
                .expression(filterExpression)
                .expressionValues(expressionValues)
                .build();

        Key k = keyBuilder.build();

        QueryEnhancedRequest queryRequest = QueryEnhancedRequest.builder()
                .queryConditional(queryFunction.apply(k))
                .filterExpression(expression)
                .build();

        final PageIterable<T> pageIterable = queryTable.query(queryRequest);
        pageIterable.stream().forEach(p -> p.items().forEach(item -> result.add(item)));
        return result;
    }

    public static <T> List<T> querySortKeyBeginsWith(T obj, String filterExpression, boolean scanIndexForward){
        return queryTable(obj,filterExpression, scanIndexForward , k -> QueryConditional.sortBeginsWith(k));
    }

    public static <T> List<T> querySortKeyGreaterThan(T obj, String filterExpression, boolean scanIndexForward){
        return queryTable(obj,filterExpression, scanIndexForward , k -> QueryConditional.sortGreaterThan(k));
    }

    public static <T> List<T> getItem(T obj){
        return queryTable(obj,null, false , k -> QueryConditional.keyEqualTo(k));
    }




    }
