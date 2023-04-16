package ipiad;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.map.JsonMappingException;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeoutException;

public class ParserController extends Thread {
    public static Logger log = LogManager.getLogger();
    private Channel channel;
    static String exchangeName = "";
    static String queueConsume = "parse_queue";
    static String queueProduce = "download_queue";

    static String queueElk = "elk_queue";

    static String consumerTag = "myConsumerTag";
    static String baseUrl = "https://www.gazeta.ru";

    Connection conn;

    public ParserController(RabbitCreds rabbitCreds) throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(rabbitCreds.username);
        factory.setPassword(rabbitCreds.password);
        factory.setVirtualHost(rabbitCreds.virtualHost);
        factory.setHost(rabbitCreds.host);
        factory.setPort(rabbitCreds.port);
        this.conn = factory.newConnection();
        this.channel = this.conn.createChannel();
        this.channel.queueDeclare(queueProduce, false, false, false, null);
        this.channel.queueDeclare(queueElk, false, false, false, null);
    }

    @Override
    public void run() {
        try {
            channel.basicConsume(queueConsume, false, consumerTag, new DefaultConsumer(channel) {
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    long deliveryTag = envelope.getDeliveryTag();
                    String message = new String(body, StandardCharsets.UTF_8);
                    List<String> urls = parseDocument(message);
                    log.info("Parsing new html");
                    for (String url_ : urls) {
                        log.info("Producing new url: " + url_);
                            publishToRMQ(url_, queueProduce);
                    }
                    Article article = getArticle(message);
                    if (article!= null) {
                        // convert user object to json string and return it
                        String jsonString = article.toJson().toString();
                        publishToRMQ(jsonString, queueElk);
                    }
                    channel.basicAck(deliveryTag, false);
                }
            });
        } catch (Exception e) {
            log.error(e);
        }
    }

    public void publishToRMQ(String element, String queuePublish) {
        byte[] messageBodyBytes = element.getBytes();
        log.info("Publishing to queue: " + queuePublish);
        Channel channel;
        try {
            channel = this.conn.createChannel();
        } catch (IOException e) {
            log.error(e);
            return;
        }
        try {
//            channel.queueDeclare(queuePublish, false, false, false, null);
            channel.basicPublish(
                    exchangeName,
                    queuePublish,
                    false,
                    MessageProperties.PERSISTENT_TEXT_PLAIN, messageBodyBytes);
        } catch (Exception e) {
            log.error(e);
        }
        try {
            channel.close();
        } catch (Exception e) {
            log.error(e);
        }
    }


    public Article getArticle(String doc) {
        try {
            Document parsedDoc = Jsoup.parse(doc);
            String timeTag = parsedDoc.getElementsByTag("time").first().attr("datetime");
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX", Locale.ENGLISH);
            Date date = formatter.parse(timeTag);
            String title = parsedDoc.getElementsByTag("title").first().text();
            String url = parsedDoc.select("meta[property=og:url]").first().attr("content");
            String author = parsedDoc.getElementsByClass("author-item").first().select("a").first().text();
            Elements contents = parsedDoc.getElementsByClass("b_article-text").select("p");
            String content = "";
            for (Element element : contents) {
                content += element.text() + "\n";
            }
            return new Article(title, author, url, date, content);
        } catch (Exception ex) {
            log.error(ex);
        }
        return null;
    }

    public List<String> parseDocument(String doc) {
        List<String> urls = new ArrayList();
        try {
            Document parsedDoc = Jsoup.parse(doc);
            Elements aTag = parsedDoc.getElementsByClass("w_col2").
                    select("a");
            for (Element element : aTag) {
                try {
                    String link = element.attr("href");
//                    log.info(element.text());
                    if (!link.startsWith("https://") && !link.startsWith("http://")) {
                        link = baseUrl + link;
                    }
                    urls.add(link);
                } catch (Exception e) {
                    log.error(e);
                }
            }
        } catch (Exception ex) {
            log.error(ex);
        }
        return urls;
    }
}
