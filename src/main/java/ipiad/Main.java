package ipiad;

import com.typesafe.config.Config;
import org.apache.log4j.varia.NullAppender;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.concurrent.TimeoutException;

import com.typesafe.config.ConfigFactory;

public class Main {

    private static Logger log = LogManager.getLogger();

    public static void main(String[] args) throws IOException, URISyntaxException, TimeoutException, ParseException {
        org.apache.log4j.BasicConfigurator.configure(new NullAppender());
        Config conf = ConfigFactory.load();

        int downloadThreads = 1;
        int parseThreads = 1;
        int elkThreads = 1;


        for (int i = 0; i < downloadThreads; i++) {
            DownloadController downloadThread = new DownloadController(conf.getConfig("rabbit"));
            downloadThread.start();
        }
        for (int i = 0; i < parseThreads; i++) {
            ParserController parseThread = new ParserController(conf.getConfig("rabbit"));
            parseThread.start();
        }
        for (int i = 0; i < elkThreads; i++) {
            ELKController elkThread = new ELKController(conf.getConfig("rabbit"), conf.getConfig("elastic"));
            elkThread.start();
        }
    }
}