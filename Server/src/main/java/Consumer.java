import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Consumer class that consume messages sent from the RabbitMQ and create an entry in hashmap
 * for each unique word received.
 */
public class Consumer {
  private final static String QUEUE_NAME = "hw3Queue";

  private static int NUM_THREADS;
  private static int BATCH_NUM;
  private static Connection conn = null;
  public static List<Object> objectsToWrite = new CopyOnWriteArrayList<>();


  public static void main(String[] args) throws Exception {
    NUM_THREADS = Integer.parseInt(args[0]);
    BATCH_NUM = Integer.parseInt(args[1]);
    Map<String, String> resultsMap = new ConcurrentHashMap<>();

    if (conn == null) {
      ConnectionFactory factory = new ConnectionFactory();
      //factory.setHost("localhost");
      factory.setHost("34.205.43.112");
      factory.setPort(5672);
      factory.setUsername("admin");
      factory.setPassword("admin");
      conn = factory.newConnection();
    }

    Runnable runnable = new Runnable() {

      @Override
      public void run() {
        try {
          Channel channel = conn.createChannel();
          channel.queueDeclare(QUEUE_NAME, true, false, false, null);
          // max one message per receiver
          channel.basicQos(1);
          //System.out.println(" [*] Thread waiting for messages. To exit press CTRL+C");
          DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            String message = new String(delivery.getBody(), "UTF-8").replaceAll("\\\"","");
            try {
              String[] values = message.split(" , ");
//              String word = values[0];
//              String count = values[1];
              WordItem wordItem = new WordItem();
              wordItem.setWord(values[0]);
              wordItem.setCount(values[1]);
              objectsToWrite.add(wordItem);
              if(objectsToWrite.size() == BATCH_NUM) {
                ItemDao.batchWrite(objectsToWrite);
                objectsToWrite.clear();
              }
//            if (resultsMap.containsKey(values[0])) {
//              String value = resultsMap.get(values[0]);
//              if (value != null) {
//                resultsMap.computeIfPresent(values[0], (key, oldValue) -> oldValue + values[1]);
//              }
//            } else {
//              resultsMap.put(values[0], values[1]);
//            }
            }finally {
              channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            }
          };
          channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> { });
          //channel.close();
        } catch (Exception ex) {
        }
      }
    };

    //Start threads and block to receive messages
    for (int i = 0; i < NUM_THREADS; i++) {
      Thread recv = new Thread(runnable);
      recv.start();
      recv.join();
    }
  }
}
