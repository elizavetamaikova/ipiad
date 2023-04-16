package ipiad;

import org.apache.log4j.varia.NullAppender;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jsoup.nodes.Document;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.Executors;

public class Main {

    private static Logger log = LogManager.getLogger();

    public static void main(String[] args) throws IOException, URISyntaxException, TimeoutException {
        org.apache.log4j.BasicConfigurator.configure(new NullAppender());

        String usernameRMQ = "rabbitmq";
        String passwordRMQ = "rabbitmq";
        String virtualHostRMQ = "/";
        String hostRMQ = "127.0.0.1";
        int portRMQ = 5672;

        RabbitCreds rabbitCreds = new RabbitCreds(usernameRMQ,
                passwordRMQ,
                virtualHostRMQ,
                hostRMQ,
                portRMQ);

        int downloadThreads = 4;
        int parseThreads = 2;

        for (int i = 0; i < downloadThreads; i++) {
            DownloadController downloadThread = new DownloadController(rabbitCreds);
            downloadThread.start();
        }
        for (int i = 0; i < parseThreads; i++) {
            ParserController parseThread = new ParserController(rabbitCreds);
            parseThread.start();
        }

//        Thread downloadController = new Thread(new DownloadController(rabbitCreds));
//        downloadController.start();
//        Thread parserController = new Thread(new ParserController(rabbitCreds));
//        parserController.start();
//        String sUrl = "https://gazeta.ru/?utm_source=vsesmi_online";
//        URL url = new URL(sUrl);
//        Document doc = downloadController.getUrl(url);
//
//        List<String> urls = parserController.parseDocument(doc);
//        log.info(urls);
    }
}