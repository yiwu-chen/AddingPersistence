import com.google.gson.Gson;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import javax.servlet.ServletException;
import javax.servlet.annotation.*;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
//import java.util.logging.Level;
//import java.util.logging.Logger;
import java.util.stream.Collectors;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;


/**
 * The servlet class that has doGet and doPost methods to send the response to the client.
 */
@WebServlet(name = "Servlet", value = "/Servlet")
public class Servlet extends HttpServlet {
  private Gson gson = new Gson();
  private Connection conn = null;
  private PooledObjectFactory channelFactory;
  private GenericObjectPool<Channel> channelPool;
  private GenericObjectPoolConfig config;

  private final static String QUEUE_NAME = "hw3Queue";


  @Override
  public void init(){
    if (conn == null) {
      ConnectionFactory factory = new ConnectionFactory();
      //factory.setHost("localhost");
      factory.setHost("34.205.43.112");
      factory.setPort(5672);
      factory.setUsername("admin");
      factory.setPassword("admin");
      try {
        conn = factory.newConnection();
        config.setMaxTotal(5000);
        channelPool.setMaxIdle(2000);
        channelFactory = new ChannelFactory(conn);
        channelPool = new GenericObjectPool<>(channelFactory,config);

//        channelPool.setMaxTotal(1000);
//        channelPool.setMaxIdle(500);

      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * The method that send the GET response.
   *
   * @param req the http servlet request
   * @param res the http servlet response
   * @throws IOException when an input or output operation is failed or interpreted
   */
  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse res) throws IOException {
    res.setContentType("application/json");
    res.setCharacterEncoding("UTF-8");
    String urlPath = req.getPathInfo();
    //System.out.println(urlPath);

    PrintWriter out = res.getWriter();

    if (urlPath == null || urlPath.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      out.write("missing parameters");
      return;
    }
    String[] urlParts = urlPath.split("/");
    if (!isUrlValid(urlParts)) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
    } else {
      String key = urlParts[3];
      //System.out.println(key);
      int result = ItemDao.getWordCount(key);
      String jsonString = new Gson().toJson(result);

      res.getWriter().write(jsonString);
      res.setStatus(HttpServletResponse.SC_OK);
    }
  }

  /**
   * The method that send the POST response.
   *
   * @param req the http servlet request
   * @param res the http servlet response
   * @throws IOException when an input or output operation is failed or interpreted
   */
  @Override
  protected void doPost(HttpServletRequest req, HttpServletResponse res) throws IOException {
    res.setContentType("application/json");
    res.setCharacterEncoding("UTF-8");

    String urlPath = req.getPathInfo();
    PrintWriter out = res.getWriter();

//    ResultVal resultVal = new ResultVal();
//    ErrMessage errMessage = new ErrMessage();

    if (urlPath == null || urlPath.isEmpty()) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      out.write("url is empty or null");
//      errMessage.setMessage("Null or empty URL");
      out.write(gson.toJson("Null or empty URL"));
      return;
    }
    String[] urlParts = urlPath.split("/");

    if (!isUrlValid(urlParts)) {
      res.setStatus(HttpServletResponse.SC_NOT_FOUND);
      out.write(gson.toJson("Invalid url"));
      return;
    }

    String requestData = req.getReader().lines().collect(Collectors.joining());
    //System.out.println(requestData);
    String message = requestData.substring(12, requestData.length() - 2).trim();
    Map<String, Integer> wordsMap = new HashMap<>();
    WordCount.wordCount(wordsMap, message);
    Channel channel = null;
    try {
      //channel = channelPool.borrowObject();
      channel = conn.createChannel();
      channel.queueDeclare(QUEUE_NAME, true, false, false, null);
      for (Map.Entry<String, Integer> entry : wordsMap.entrySet()) {
        StringBuilder tuple = new StringBuilder();
        tuple.append(entry.getKey());
        tuple.append(" , ");
        tuple.append(entry.getValue());
        channel.basicPublish("", QUEUE_NAME, null, gson.toJson(tuple.toString()).getBytes(StandardCharsets.UTF_8));
      }

    } catch (Exception ex) {
      // Logger.getLogger(Servlet.class.getName()).log(Level.SEVERE, null, ex);
    } finally {
      if (channel != null) {
        try {
          //channelPool.returnObject(channel);
          channel.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
    }
    int totalUniqueWords = wordsMap.size();

    out.write(gson.toJson(totalUniqueWords));
    res.setStatus(HttpServletResponse.SC_OK);

    out.close();
    out.flush();
    //conn.close();
  }

  /**
   * Validate the url to see if it is valid.
   * Currently, only support the wordCount function, so only textboday/wordcount is valid.
   *
   * @param urlParts all the parts of the url
   * @return result of the validation
   */
  private boolean isUrlValid(String[] urlParts) {
    if (urlParts == null) {
      return false;
    }
    return urlParts[1].equals("textbody");
  }
}
