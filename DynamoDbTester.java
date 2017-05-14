public class DynamoDbTester {
  public static void main(String...args) throws Exception{
    /*ObjectBuilder objBuilder = new ObjectBuilder();
    String jsonObject = objBuilder.buildJsonObject("0,7.0,7.0,8.0,8.0", ",");
    
    System.out.println(jsonObject);*/
    
    DynamoDbBridge bridge = new DynamoDbBridge("tf-quadtree-main-bucket", "aws1.csv", "tf-quadtree-dynamodb");
  }
}