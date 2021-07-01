import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBScanExpression;
import com.amazonaws.services.dynamodbv2.document.spec.GetItemSpec;

import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.spec.UpdateItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

public class ItemDao {
  private static AmazonDynamoDB client;
  private static DynamoDB dynamoDB;
  private static Table table;
  private static DynamoDBMapper mapper;

  static {
    BasicSessionCredentials sessionCredentials = new BasicSessionCredentials(
            "ASIA22HOWFS5RL6FUD46",
            "jubkNW1/U77vzGd60yebyr7oCxGNnlWM+0ffcBKz", "FwoGZXIvYXdzEO3//////////wEaDLlnRPbMjsVdMsZf6SLLAXYCMR6lNP13/mjsxHfjZB48Wcolaom96lmx1qR2h7mRFeBkwWQ2xrv+OOHNA0VmF4hZPOPfXp7QYbTdcU9aSvrS8Tkxk88I11+9FKwfdAac1zghdewRFkWUxkEIr710TjAVofhyw6wJobBwVF6xQUy0Bw4uiUtNTStzDbkAG9DXW0+X/4uIma2aa/URTef8Rux6weqYLUKqa6GXrluNkkLBdGaKwmGl+2vkgpvnOq5weY3WDjFj4Ep+Lqh9R16NswXI0WzikQpyn7NLKKX69IYGMi1lB3PP46bo5r4jznBmQ/EOgWV0YYpVfQwcSWAgj74O0nJpaNsTdfEbETjrjQU=");
    // ClientConfiguration clientConfiguration = new ClientConfiguration();
//    clientConfiguration.setMaxConnections(6000);
//    clientConfiguration.setMaxErrorRetry(3);

    client = AmazonDynamoDBClientBuilder.standard()
            //.withClientConfiguration(clientConfiguration)
            .withCredentials(new AWSStaticCredentialsProvider(sessionCredentials))
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("dynamodb.us-east-1.amazonaws.com", "us-east-1"))
            .build();
    //System.out.println("Connection Created");
    dynamoDB = new DynamoDB(client);
    table = dynamoDB.getTable("WordTable2");
    mapper = new DynamoDBMapper(client);
  }

  public static void createItems(String word, int count) {
    try {
      Item item = new Item().withPrimaryKey("Word", word)
              .withNumber("Count", count);
      table.putItem(item);
      //System.out.println(item.toJSONPretty());
    } catch (Exception e) {
      System.err.println("Create items failed.");
      System.err.println(e.getMessage());
    }
  }

  public static void createItemsUUID(String word, int count) {
    try {

      String uuid = UUID.randomUUID().toString();
      Item item = new Item().withPrimaryKey("UUID", uuid).withString("Word", word).withNumber("Count", count);
      table.putItem(item);
      //System.out.println(item.toJSONPretty());
    } catch (Exception e) {
      System.err.println("Create items failed.");
      System.err.println(e.getMessage());
    }
  }

  public static String retrieveItem(String word) {
    try {
      GetItemSpec spec = new GetItemSpec().withPrimaryKey("Word", word);
      Item item = table.getItem(spec);
      //System.out.println(item.toJSONPretty());
      return item.get("Count").toString();
    } catch (Exception e) {
      e.printStackTrace();
    }
    return null;
  }

  public static String retrieveItemCount(String word) {

//    HashMap<String, String> nameMap = new HashMap<String, String>();
//    nameMap.put("#Word", "Word");
//
//    HashMap<String, Object> valueMap = new HashMap<String, Object>();
//    valueMap.put(":w", word);


    ScanSpec scanSpec = new ScanSpec().withProjectionExpression("#Word, Count")
            .withFilterExpression("#Word = :w").withNameMap(new NameMap().with("#Word", "Word"))
            .withValueMap(new ValueMap().withString(":w", word));

    try {
      ItemCollection<ScanOutcome> items = table.scan(scanSpec);

      Iterator<Item> iter = items.iterator();
      while (iter.hasNext()) {
        Item item = iter.next();
        System.out.println(item.toString());
      }
    } catch (Exception e) {
      System.err.println("Unable to scan the table:");
      System.err.println(e.getMessage());
    }
    return null;
  }

  /**
   * You can use updateItem to implement an atomic counter, where you increment or decrement
   * the value of an existing attribute without interfering with other write requests.
   *
   * @param word
   * @param count
   */
  public static void updateItem(String word, int count) {
    Map<String, String> expressionAttributeNames = new HashMap<String, String>();
    expressionAttributeNames.put("#p", "Count");

    Map<String, Object> expressionAttributeValues = new HashMap<String, Object>();
    expressionAttributeValues.put(":val", count);

    UpdateItemOutcome outcome = null;
    try {
      UpdateItemSpec updateItemSpec = new UpdateItemSpec()
              .withPrimaryKey(new PrimaryKey("Word", word)).withUpdateExpression("set #p = #p + :val")
              .withConditionExpression("attribute_exists(Word)")
              .withNameMap(expressionAttributeNames).withValueMap(expressionAttributeValues)
              .withReturnValues(ReturnValue.UPDATED_NEW);
      outcome = table.updateItem(updateItemSpec);
    } catch (Exception ex) {
      //ex.printStackTrace();
    }
    if (outcome == null) {
      createItems(word, count);
    }
  }

  public static String gueryTable(String key) {
    Map<String, String> nameMap = new HashMap<>();
    nameMap.put("#p", "Count");

    Map<String, Object> valueMap = new HashMap<>();
    valueMap.put(":val", new AttributeValue().withS(key));

    QuerySpec spec = new QuerySpec()
            .withKeyConditionExpression("Word = :v_id")
            .withValueMap(new ValueMap()
                    .withString(":v_id", key));


    ItemCollection<QueryOutcome> items = table.query(spec);

    Iterator<Item> iterator = items.iterator();
    Item item = null;
    while (iterator.hasNext()) {
      item = iterator.next();
      System.out.println(item.toJSONPretty());
    }
    return null;
  }


  public static void writeMultipleItemsBatchWrite(String word, int count) {
    try {

      // Add a new item to Forum
      TableWriteItems forumTableWriteItems = new TableWriteItems("WordTable") // Forum
              .withItemsToPut(new Item().withPrimaryKey("Word", word).withNumber("Count", count));

      //System.out.println("Making the request.");
      BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(forumTableWriteItems);

      do {

        // Check for unprocessed keys which could happen if you exceed
        // provisioned throughput

        Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();

        if (outcome.getUnprocessedItems().size() == 0) {
          // System.out.println("No unprocessed items found");
        } else {
          //System.out.println("Retrieving the unprocessed items");
          outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
        }

      } while (outcome.getUnprocessedItems().size() > 0);

    } catch (Exception e) {
      System.err.println("Failed to retrieve items: ");
      e.printStackTrace(System.err);
    }
  }

  public static void batchWrite(List<Object> objectsToWrite) {
    mapper.batchSave(objectsToWrite);
  }

  public static void batchDelete(List<Object> objectsToDelete) {
    mapper.batchDelete(objectsToDelete);
  }


  public static void test() {
    WordItem item1 = new WordItem();
    item1.setCount("1");
    item1.setWord("Hello");
    mapper.save(item1);
  }

  public static int getWordCount(String word) {
    int numberOfThreads = 4;

    Map<String, AttributeValue> eav = new HashMap<String, AttributeValue>();
    eav.put(":n", new AttributeValue().withS(word));

    DynamoDBScanExpression scanExpression = new DynamoDBScanExpression()
            .withFilterExpression("Word = :n")
            .withExpressionAttributeValues(eav);

    List<WordItem> scanResult = mapper.parallelScan(WordItem.class, scanExpression, numberOfThreads);
    int count = 0;
    for (WordItem wordItem : scanResult) {
      count += Integer.parseInt(wordItem.getCount());
    }
    //System.out.println("Count: " + count);
    return count;
  }

}