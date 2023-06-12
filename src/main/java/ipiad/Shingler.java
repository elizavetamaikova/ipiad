package ipiad;

import com.typesafe.config.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.codelibs.minhash.MinHash;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class Shingler extends Thread {
    public static Logger log = LogManager.getLogger();
    public static ELKConnector elkConnector;
    public static int shingleLength = 5;

    public Shingler(Config elkConfig) throws UnknownHostException {
        this.elkConnector = new ELKConnector(elkConfig);
    }

    public List<String> splitStringToShingles(String text) {
        Predicate<CharSequence> stopper = null;
        List<String> shingles = new ArrayList<String>();

        for (int i = 0; i < (text.length() - shingleLength + 1); i++) {
            CharSequence s = text.subSequence(i, i + shingleLength);

            if (stopper != null && stopper.test(s)) {
                // shingle matches a stopword, so skip it
                continue;
            }

            shingles.add(s.toString());
        }

        return shingles;
    }

    public class MinHashProducer {
        public int seed = 42;
        public int numHashes = 128;
        public int hashBit = 1;
        Tokenizer tokenizer = new WhitespaceTokenizer();
        Analyzer analyzer = MinHash.createAnalyzer(tokenizer, hashBit, seed, numHashes);

        String calculateMinHash(String text) throws IOException {
            byte[] minhash = MinHash.calculate(analyzer, text);
            return bytesToHexString(minhash);
        }

        private static String bytesToHexString(byte[] byteArray) {
            StringBuilder sb = new StringBuilder();
            for (byte b : byteArray) {
                sb.append(String.format("%02X", b));
            }
            return sb.toString();
        }
    }

    public List<String> convertShinglesToMinHashList(List<String> shingles) throws IOException {
        List<String> hashedShingles = new ArrayList<String>();
        for (String shingle : shingles) {
            String hash = new MinHashProducer().calculateMinHash(shingle);
            hashedShingles.add(hash);
        }
        return hashedShingles;
    }

    @Override
    public void run() {
        log.info("Starting (Shingler)");
        while (true) {
            try {
                SearchResponse searchResponse = elkConnector.findNotExistField("shingles");
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits) {
                    Map<String, Object> source = hit.getSourceAsMap();
                    String content = (String) source.get("content");
                    List<String> shingles = splitStringToShingles(content);
                    List<String> hashedShinges = convertShinglesToMinHashList(shingles);
                    elkConnector.updateDocument(hit.getId(), hashedShinges);
                }
            } catch (IOException e) {
                log.error(e);
            }
        }
    }
}
