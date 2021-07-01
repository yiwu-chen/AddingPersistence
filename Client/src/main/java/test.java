import java.math.BigDecimal;

import io.swagger.client.ApiException;
import io.swagger.client.api.TextbodyApi;

public class test {
  public static void main(String[] args) {
    TextbodyApi api = new TextbodyApi();


    Thread getThread = new Thread(new Runnable() {
      @Override
      public void run() {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
        String word = "word_example";
        try {
          BigDecimal result = api.getWordCount(word);
        } catch (ApiException e) {
          System.err.println("Exception when calling TextbodyApi#getWordCount");
          e.printStackTrace();
        }
      }
    });

    getThread.start();
  }
}
