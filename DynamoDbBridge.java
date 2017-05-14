import java.util.List;
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

public class DynamoDbBridge {
  private ObjectBuilder objBuilder;
  private String bucketName, fileName, S3Object, dynamoDbTableName;
  private AWSCredentials credentials;
  private Region awsRegion;
  private AmazonS3 s3;
  private AmazonDynamoDBClient dynamoDb;
  private boolean S3BucketExists;
  
  public DynamoDbBridge(String bucketName, String key, String tableName) throws Exception{
    this.objBuilder = new ObjectBuilder();
    this.bucketName = bucketName;
    this.fileName = key;
    this.S3Object = this.getS3Object(this.bucketName, this.fileName);  
    this.dynamoDbTableName = tableName;
    this.S3BucketExists = false;
    
    this.initAws(true);
  }
  
  public void crossBridge(String bucketName, String key) throws Exception{
    String s3Object = this.getS3Object(bucketName, key);
    System.out.println(s3Object);
    //String dynamoDbObject = this.buildDynamoDbObject(s3Object);
        
    //this.pushToDynamoDb(dynamoDbObject);
  }  
  
  private String getS3Object(String bucketName, String key) throws IOException{
    S3Object object = this.s3.getObject(new GetObjectRequest(bucketName, key));
    String node = "";
    
    try{
      node = this.buildObjectFromTextInputStream(object.getObjectContent());
    }
    catch(IOException e){
      System.out.println(e);
    }
    
    return node;
  }  
  
  public String buildDynamoDbObject(String s3Object){
    String dynamoDbObject = this.objBuilder.buildJsonObject(s3Object, ",");
    
    return dynamoDbObject;
  }
  
  public void pushToDynamoDb(String JSON){
    /* Push the JSON to dynamoDB */
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
    this.dynamoDb = new AmazonDynamoDBClient(credentials);
    this.awsRegion = Region.getRegion(Regions.US_WEST_2);
    
    this.s3.setRegion(this.awsRegion);
    this.dynamoDb.setRegion(this.awsRegion);
    
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
        
        TableUtils.createTableIfNotExists(this.dynamoDb, createTableRequest);
        TableUtils.waitUntilActive(this.dynamoDb, this.dynamoDbTableName);
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
  
  private String buildObjectFromTextInputStream(InputStream input) throws IOException { //https://github.com/aws/aws-sdk-java/blob/master/src/samples/AmazonS3/S3Sample.java
    BufferedReader reader = new BufferedReader(new InputStreamReader(input));
    String line = "";
    
    while(true){
      line = reader.readLine();
      if(line == null) break;
    }
    
    return line;
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
  
  /*private void populateS3Keys(){
    final ListObjectsV2Request req = new ListObjectsV2Request().withBucketName(this.bucketName).withMaxKeys(2);
    ListObjectsV2Result result;
    boolean isResultNotInitialized = true;
    
    result = this.s3.listObjectsV2(req);
    
    while(result.isTruncated()){
      if(!isResultNotInitialized){               
        result = this.s3.listObjectsV2(req);
      }
      else{
        isResultNotInitialized = false;
      }
               
      for(S3ObjectSummary currentObject : result.getObjectSummaries()){
        this.s3KeyList.add(currentObject.getKey());
      }
      
      req.setContinuationToken(result.getNextContinuationToken());
    }
  }*/
}