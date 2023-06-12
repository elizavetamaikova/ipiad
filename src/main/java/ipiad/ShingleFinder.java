package ipiad;

import com.typesafe.config.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.io.IOException;
import java.lang.reflect.Array;
import java.net.UnknownHostException;
import java.util.*;

public class ShingleFinder {
    public static Logger log = LogManager.getLogger();
    public static ELKConnector elkConnector;

    public ShingleFinder(Config elkConfig) throws UnknownHostException {
        this.elkConnector = new ELKConnector(elkConfig);
    }


    public void findSimilarDocuments(int numberOfCommonElements) {
        try {
            SearchResponse searchResponse = elkConnector.getAllDocuments();
            SearchHits hits = searchResponse.getHits();
            for (SearchHit hit : hits) {
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                List<String> shingles = (List<String>) sourceAsMap.get("shingles");
                Set<String> shinglesSet = new HashSet<>(shingles);
                ArrayList<String> commonShingles = new ArrayList<String>();
                for (SearchHit hit_ : hits) {
                    if (hit.getId() == hit_.getId()) {
                        continue;
                    }

                    Map<String, Object> sourceAsMap_ = hit_.getSourceAsMap();
                    List<String> shingles_ = (List<String>) sourceAsMap_.get("shingles");
                    Set<String> shinglesSet_ = new HashSet<>(shingles_);

                    Set<String> commonElements = new HashSet<>(shinglesSet);
                    commonElements.retainAll(shinglesSet_);
                    int commonElementsCount = commonElements.size();
                    boolean b = commonElementsCount > (Math.min(shinglesSet.size(), shinglesSet_.size()) * numberOfCommonElements / 1000);
                    if (b) {
                        commonShingles.add((String) sourceAsMap_.get("title"));
                    }
                }
                if (commonShingles.size() > 1) {
                    log.info("================================");
                    log.info(sourceAsMap.get("title") + " :");
                    for (String key : commonShingles){
                        log.info(key);
                    }
                    log.info("================================");
                }
            }
        } catch (IOException e) {
            log.error(e);
        }
    }
}
