public class DynamoBridge {
  private ObjectBuilder objBuilder;
  private String bucketName, fileName, S3Object, dynamoDbName;
  
  public DynamoBridge(String bucketName, String key, String dbName) {
    this.objBuilder = new objBuilder();
    this.bucketName = bucketName;
    this.fileName = key;
    this.S3Object = this.getS3Object(this.bucketName, this.fileName);  
    this.dynamoDbName = dbName;
  }
  
  public String buildDynamoDbObject() {
    /* Build the JSON with this.objBuilder and then return the JSON */
    return "";
  }
  
  public void pushToDynamoDb(String JSON) {
    /* Push the JSON to dynamoDB */
  }
  
  private String getS3Object(String bucketName, String key) {
    /* Make a connection to S3 bucketName here and get the file */
    
    /* Read in the object, save it to a String, and return that String */
    
    return "";
  }
}