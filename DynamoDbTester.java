public class DynamoDbTester {
  public static void main(String...args){
    ObjectBuilder objBuilder = new ObjectBuilder();
    String jsonObject = objBuilder.buildJsonObject("0,7.0,7.0,8.0,8.0", ",");
    
    System.out.println(jsonObject);
  }
}