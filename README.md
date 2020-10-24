# DynamoDB Database Helper using Enhanced Client

DynamoDB Database Helper - refer to package com.ppe.db.helper.  
Ignore other packages because those packages use code from Old DynamoDB CLient and DynamoMapper.  The "EHelper" class in package com.ppe.db.helper is the primary one with all functions for transactions, get, put and query.

I am using FakeCustomer class in the same package com.ppe.db.helper which has few annotations applied.  Some of these annotations are provided by Enhanced Client Library.  However, there are few which i have custom built.  These annotations are also part of package com.ppe.db.helper package.


Examples 

1. Get Item 

 ```Java
        FakeCustomer mc1 = new FakeCustomer();
        mc1.setPARTITION_KEY("Test");
        mc1.setSORT_KEY("1");
        
        FakeCustomer item2 = EHelper.getItem(mc1);
  ```
        
2. Put Item (Insert without Transaction)

 ```Java
        EHelper.putItem(mc1 , FakeCustomer.class);
 ```

3. Transactions 

    -  Inserts will check if partition and sort key already exists.  It will throw error and transaction will fail
    -  Updates will work by getting object first and the update will overwrite previous fields.  The values which will be null in the object , will be ignored while updating and original values will be retained
    -  Optimistic Updates : All objects must have an Integer field, which must be marked with annotation @DynamoDbVersionAttribue.  This attribute will be checked while updating.  If another process has updated the item, then your update will not be applied and transaction will fail.  You should have a retry mechanism with revised calculations
    
 ```Java
 
    // Both insert and update
    // Create Transaction Packet
      
      EHelper.TxnPacket packet = EHelper.TxnPacket.builder()
      .update(mc1, FakeCustomer.class,null)
      .insert(mc2,FakeCustomer.class)
      .insert(mc3, FakeCustomer.class)
      .build();
       
    // Execute Transaction
        EHelper.executeTransactionWrite(packet);
     
```
   
        
4. Other Query options

Currently Helper only supports query on tables.  This needs to be extended for Indexes.  There are mutliple overloaded functions with ability to search with    variety of options.
Function examples :  Search for fakecustomer with partition_key = Test and sort key greater than 1

 ```Java
        FakeCustomer mc1 = new FakeCustomer();
        mc1.setPARTITION_KEY("Test");
        mc1.setSORT_KEY("1");
        
        List<FakeCustomer> fakeCustomers = EHelper.querySortKeyGreaterThan(mc1, null, false);
  ```   
        
 5. Annotations on Model 
 
 * @DynamoDbBean :  Class level,  all beans must be marked.  Provided by Enhanced Client
 * @Table(name = "ACCOUNT") :  Class level.  Custom for Puma. Helps identify the table to be used for this object
 * @PartitionKey : fields level.  Custom for Puma. Helps identify paritition key for this object
 * @SortKey : fields level.  Custom for Puma. Helps identify paritition key for this object
 * @DynamoDbVersionAttribute : method level. Provided by Enhanced Client. helps identify version attribute
 * @DynamoDbPartitionKey , @DynamoDbSortKey : method level. Provided by Enhanced Client. Used by Enhanced client for identification of primary key
 * @DynamoDbAttribute("Pictures") : method level. Provided by Enhanced Client. If dynamodb column name is different from object property name
 * @DynamoDbIgnore :  Ignore field for update and insert
 * @DynamoDbUpdateBehavior(UpdateBehavior.WRITE_IF_NOT_EXISTS) :  Interesting update behaviour, to be used for created_date kind of option

 
 
```Java
 
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
 ```
 
 



      


