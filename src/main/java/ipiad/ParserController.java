package ipiad;

import com.rabbitmq.client.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

public class ParserController extends Thread {
//public class ParserController {
    public static Logger log = LogManager.getLogger();
    private Channel channel;
    static String exchangeName = "";
    static String queueConsume = "parse_queue";
    static String queueProduce = "download_queue";

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
        this.channel.queueDeclare("queuePublish", false, false, false, null);
    }

    @Override
    public void run() {
        try {
            channel.basicConsume(queueConsume, false, consumerTag, new DefaultConsumer(channel) {
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    long deliveryTag = envelope.getDeliveryTag();
                    String message = new String(body, StandardCharsets.UTF_8);
                    List<String> urls = parseDocument(message);
//                    publishToRMQ(message, "queuePublish");
                    log.info("Parsing new html");
                    for (String url_:urls) {
                        log.info("Producing new url: " + url_);
                        publishToRMQ(url_, queueProduce);
                    }
                    channel.basicAck(deliveryTag, false);
                }
            });
        } catch (Exception e) {
            log.error(e);
        }
    }

    public void publishToRMQ (String element, String queuePublish) {
        byte[] messageBodyBytes = element.getBytes();
        try {
            channel.queueDeclare(queuePublish, false, false, false, null);
            channel.basicPublish(
                    exchangeName,
                    queuePublish,
                    false,
                    MessageProperties.PERSISTENT_TEXT_PLAIN, messageBodyBytes);
        } catch (Exception e) {
            log.error(e);
        }
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
