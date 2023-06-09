package ipiad;

import com.rabbitmq.client.*;
import com.typesafe.config.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.util.concurrent.TimeoutException;

public class ELKController extends Thread {
    public static Logger log = LogManager.getLogger();
    static String queueElk = "elk_queue";
    static String elkIndex = "gazeta";
    static String consumerTag = "myConsumerTag";

    ELKConnector elkConnector;

    private Connection conn;
    private Channel channel;

    public ELKController(Config rabbitConfig, Config elkConfig) throws IOException, TimeoutException {
        this.elkConnector = new ELKConnector(elkConfig);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(rabbitConfig.getString("user"));
        factory.setPassword(rabbitConfig.getString("password"));
        factory.setVirtualHost(rabbitConfig.getString("virtualHost"));
        factory.setHost(rabbitConfig.getString("host"));
        factory.setPort(rabbitConfig.getInt("port"));
        this.conn = factory.newConnection();
        this.channel = this.conn.createChannel();
        this.channel.queueDeclare(queueElk, false, false, false, null);
    }

    @Override
    public void run() {
        log.info("Starting (ELKController)");
        try {
            channel.basicConsume(queueElk, false, consumerTag, new DefaultConsumer(channel) {
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    long deliveryTag = envelope.getDeliveryTag();
                    channel.basicAck(deliveryTag, false);
                    String message = new String(body, StandardCharsets.UTF_8);
                    log.info("Got new message (ELKController)");
                    try {
                        Article article = new Article(message);
                        if (elkConnector.isUnique(article.md5))
                        {
                            elkConnector.postDocument(article);
                        }
                    } catch (ParseException e) {
                        log.error("Can't parse message: " + message, e);
                    } catch (NoSuchAlgorithmException e) {
                        log.error("Can't hash message: " + message, e);
                    }
                }
            });
        } catch (Exception e) {
            log.error(e);
        }
    }
}
