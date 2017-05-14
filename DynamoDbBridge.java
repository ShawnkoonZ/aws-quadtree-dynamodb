import java.util.List;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.util.TableUtils;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;

public class DynamoDbBridge {
  private ObjectBuilder objBuilder;
  private String bucketName, fileName, dynamoDbTableName;
  private AWSCredentials credentials;
  private Region awsRegion;
  private AmazonS3 s3;
  private AmazonDynamoDBClient dynamoDbClient;
  private DynamoDB dynamoDb;
  private Table dynamoDbTable;
  private boolean S3BucketExists;
  
  public DynamoDbBridge(String bucketName, String key, String tableName) throws Exception{
    this.objBuilder = new ObjectBuilder();
    this.bucketName = bucketName;
    this.fileName = key; 
    this.dynamoDbTableName = tableName;
    this.S3BucketExists = false;
    
    this.initAws(true);
  }
  
  public void crossBridge(String bucketName, String key) throws Exception{
    ArrayList<String> s3Objects = this.getS3Objects(bucketName, key);
    System.out.println(s3Objects);
    
    while(!s3Objects.isEmpty()){
      String s3Object = s3Objects.remove(0);
      String dynamoDbObject = this.buildDynamoDbObject(s3Object);
      String id = this.getId(dynamoDbObject);
         
      if(!id.equals("-1")){
        this.pushToDynamoDb("1", dynamoDbObject); //TODO CHANGE THE FIRST PARAM TO THE ID VALUE - IT IS "1" FOR DEBUGGING PURPOSES
      }
    }
  }  
  
  private ArrayList<String> getS3Objects(String bucketName, String key) throws IOException{
    
    S3Object object = this.s3.getObject(new GetObjectRequest(bucketName, key));
    System.out.println("DEBUG: " + object);
    ArrayList<String> s3Objects = new ArrayList<String>();
    
    try{
      s3Objects = this.buildObjectFromTextInputStream(object.getObjectContent());
    }
    catch(IOException e){
      System.out.println(e);
    }
    
    return s3Objects;
  }  
  
  public String buildDynamoDbObject(String s3Object){
    String dynamoDbObject = this.objBuilder.buildJsonObject(s3Object, ",");
    
    return dynamoDbObject;
  }
  
  public void pushToDynamoDb(String id, String JSON){     
    Item item = new Item()
      .withPrimaryKey("Id", id)
      .withJSON("Node", JSON);
      
    System.out.println("Putting in the item with ID: " + id + " and JSON " + JSON);
      
    PutItemOutcome outcome = this.dynamoDbTable.putItem(item); 
    System.out.println("DEBUG: Outcome -- " + outcome);
  }
  
  private void initAws(boolean initCross) throws Exception{
    this.credentials = null;
    
    try{
      this.credentials = new ProfileCredentialsProvider().getCredentials();
    }
    catch(Exception e){
      throw new AmazonClientException("Cannot load the credentials from the credential profiles file!\n", e);
    }
    
    this.s3 = new AmazonS3Client(this.credentials);
    System.out.println("DEBUG: this.s3 instantiated");
    this.dynamoDbClient = new AmazonDynamoDBClient(credentials);
    this.dynamoDb = new DynamoDB(this.dynamoDbClient);
    System.out.println("DEBUG: this.dynamoDb instantiated");
    this.awsRegion = Region.getRegion(Regions.US_WEST_2);
    
    this.s3.setRegion(this.awsRegion);
    System.out.println("DEBUG: this.s3.setRegion(...) called");
    this.dynamoDbClient.setRegion(this.awsRegion);
    System.out.println("DEBUG: this.s3dynamoDb.setRegion(...) called");
    
    this.S3BucketExists = this.isS3NotEmpty();
    this.initDynamoDbTable();
    
    if(initCross){
      this.crossBridge(this.bucketName, this.fileName);
    }
  }
  
  private void initDynamoDbTable() throws Exception{
    if(this.S3BucketExists){
      try{ //https://github.com/aws/aws-sdk-java/blob/master/src/samples/AmazonDynamoDB/AmazonDynamoDBSample.java
        CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(this.dynamoDbTableName)
          .withKeySchema(new KeySchemaElement().withAttributeName("name").withKeyType(KeyType.HASH))
          .withAttributeDefinitions(new AttributeDefinition().withAttributeName("name").withAttributeType(ScalarAttributeType.S))
          .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));
        
        TableUtils.createTableIfNotExists(this.dynamoDbClient, createTableRequest);
        TableUtils.waitUntilActive(this.dynamoDbClient, this.dynamoDbTableName);
        
        this.dynamoDbTable = this.dynamoDb.getTable(this.dynamoDbTableName);
      }
      catch(AmazonServiceException ase) {
        System.out.println("Caught an AmazonServiceException, which means your request made it "
                           + "to AWS, but was rejected with an error response for some reason.");
        System.out.println("Error Message:    " + ase.getMessage());
        System.out.println("HTTP Status Code: " + ase.getStatusCode());
        System.out.println("AWS Error Code:   " + ase.getErrorCode());
        System.out.println("Error Type:       " + ase.getErrorType());
        System.out.println("Request ID:       " + ase.getRequestId());
      } 
      catch(AmazonClientException ace) 
      {
        System.out.println("Caught an AmazonClientException, which means the client encountered "
                           + "a serious internal problem while trying to communicate with AWS, "
                           + "such as not being able to access the network.");
        System.out.println("Error Message: " + ace.getMessage());
      }
    }
    else{
      throw new Exception("S3 bucket \"" + this.bucketName + "\" does not exist!");
    }
  }
  
  private String getId(String node){
    String[] splitNode = node.split(",");
    
    if(splitNode.length > 0){
       return splitNode[0];
    }
    
    return "-1";
  }
  
  private ArrayList<String> buildObjectFromTextInputStream(InputStream input) throws IOException { //https://github.com/aws/aws-sdk-java/blob/master/src/samples/AmazonS3/S3Sample.java
    BufferedReader reader = new BufferedReader(new InputStreamReader(input));
    String line = "";
    ArrayList<String> object = new ArrayList<String>();;
    boolean isData = true;
    
    while(isData){
      line = reader.readLine();
      if(line == null){
        isData = false;
      }
      else{
        object.add(line);
      }
    }
    
    return object;
  }
 
  private boolean isS3NotEmpty(){
    List<Bucket> buckets = this.s3.listBuckets();
   
    for(Bucket currentBucket : buckets){
      if(currentBucket.getName().equals(this.bucketName)){
        return true;
      }
    }
   
    return false;
  }
}