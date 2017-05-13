public class ObjectBuilder {
  public ObjectBuilder() { }
  
  public String buildJsonObject(String S3Object, String splitChar) {
    String[] objProperties = S3Object.split(splitChar);
    String json = "";
    
    if(objProperties.length == 5 || objProperties.length == 6) {
      String id = objProperties[0];
      String xMin = objProperties[1];
      String yMin = objProperties[2];
      String xMax = objProperties[3];
      String yMax = objProperties[4];
      String data = (objProperties.length == 6) ? objProperties[5] : ""; 
      
      json = "{\"id\":\"" + id + "\"," + 
             "\"coordinate\":{\"xMin\":\"" + xMin + "\"," +
             "\"yMin\":\"" + yMin + "\"," +
             "\"xMax\":\"" + xMax + "\"," + 
             "\"yMax\":\"" + yMax + "\"}," +
             "\"data\":\"" + data + "\"}";
    }
    else {
      json = "{}";
    }
    
    return json;
  }
}