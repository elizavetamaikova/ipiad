package ipiad;

import com.rabbitmq.client.*;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.apache.http.client.CookieStore;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeoutException;


public class DownloadController extends Thread {
//public class DownloadController {

    public static Logger log = LogManager.getLogger();
    private CloseableHttpClient client = null;
    private HttpClientContext context;
    private URL serverURL = null;
    private List<Header> headers = new ArrayList<>();
    private int retDelay = 5 * 1000;
    private int retCount = 2;
    private int metadataTimeout = 30 * 1000;
    private Channel channel;
    static String exchangeName = "";
    static String queueConsume = "download_queue";

    static String queueProduce = "parse_queue";

    static String consumerTag = "myConsumerTag";

    Connection conn;

    public static String userAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36";

    public DownloadController(RabbitCreds rabbitCreds) throws IOException, TimeoutException {
        CookieStore httpCookieStore = new BasicCookieStore();
        client = HttpClients.custom().setUserAgent(this.userAgent).build();
        context = HttpClientContext.create();
        context.setCookieStore(httpCookieStore);

        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(rabbitCreds.username);
        factory.setPassword(rabbitCreds.password);
        factory.setVirtualHost(rabbitCreds.virtualHost);
        factory.setHost(rabbitCreds.host);
        factory.setPort(rabbitCreds.port);
        this.conn = factory.newConnection();
        this.channel = this.conn.createChannel();
        this.channel.queueDeclare(queueConsume, false, false, false, null);
    }

    @Override
    public void run() {
        try {
            channel.basicConsume(queueConsume, false, consumerTag, new DefaultConsumer(channel) {
                public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                    long deliveryTag = envelope.getDeliveryTag();
                    String message = new String(body, StandardCharsets.UTF_8);
                    log.info("Got link: " + message);
                    URL url = new URL(message);
                    try {
                        String doc = String.valueOf(getUrl(url));
                        publishToRMQ(doc, queueProduce);
                    } catch (URISyntaxException e) {
                        throw new RuntimeException(e);
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

    public String getUrl(URL url) throws URISyntaxException {
        int code = 0;
        boolean bStop = false;
        String doc = null;

        for (int iTry = 0; iTry < retCount && !bStop; iTry++) {
            log.info("getting page from url " + url);
            RequestConfig requestConfig = RequestConfig.custom()
                    .setConnectTimeout(metadataTimeout)
                    .setConnectionRequestTimeout(metadataTimeout)
                    .setExpectContinueEnabled(true)
                    .build();
            HttpGet request = new HttpGet(url.toURI());
            request.setConfig(requestConfig);
            for (Header header : headers)
                request.setHeader(header);
            CloseableHttpResponse response = null;
            try {
                response = client.execute(request, context);
                code = response.getStatusLine().getStatusCode();
                if (code == 404) {
                    log.warn("error get url" + url + " code " + code);
                    bStop = true;
                } else if (code == 200) {
                    HttpEntity entity = response.getEntity();
                    if (entity != null) {
                        doc = EntityUtils.toString(entity, "UTF-8");;
                        break;
                    }
                    bStop = true; //break
                } else {
                    if (code == 403) {
                        log.warn("error get url " + url + " code " + code);
                        Arrays.stream(response.getAllHeaders()).forEach(header -> {
                            Header eheader = headers.stream()
                                    .filter(h -> h.getName().equals(header.getName()))
                                    .findAny()
                                    .orElse(null);
                            if (eheader == null)
                                headers.add(header);
                        });
                    }
                    response.close();
                    response = null;
                    int delay = retDelay * 1000 * (iTry + 1);
                    log.info("wait " + delay / 1000 + "s...");
                    try {
                        Thread.sleep(delay);
                        continue;
                    } catch (InterruptedException ex) {
                        break;
                    }
                }
            } catch (IOException e) {
                log.error(e);
            }
            if (response != null) {
                try {
                    response.close();
                } catch (IOException e) {
                    log.error(e);
                }
            }
        }
        log.info("got page from url " + url);
        return doc;
    }
}
