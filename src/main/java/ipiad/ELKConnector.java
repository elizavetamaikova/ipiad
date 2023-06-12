package ipiad;

import com.typesafe.config.Config;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.net.UnknownHostException;
import java.security.NoSuchAlgorithmException;
import java.util.List;


public class ELKConnector {
    public static Logger log = LogManager.getLogger();
    private final RestHighLevelClient client;

    private String indexName;
    Config elkConfig;

    public ELKConnector(Config config) throws UnknownHostException {
        this.elkConfig = config;
        this.client = createClient();
        this.indexName = this.elkConfig.getString("indexName");
    }

    private RestHighLevelClient createClient() {
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(this.elkConfig.getString("user"),
                        this.elkConfig.getString("password")));
        RestClientBuilder builder = RestClient.builder(new HttpHost(this.elkConfig.getString("host"), this.elkConfig.getInt("port"), "http"));
        builder.setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        RestHighLevelClient client_ = new RestHighLevelClient(builder);
        return client_;
    }

    public void close() throws IOException {
        client.close();
    }

    void postDocument(Article article) throws IOException, NoSuchAlgorithmException {
        IndexRequest request = new IndexRequest(indexName, "doc");
        request.source(article.toHashMap());

        client.index(request, RequestOptions.DEFAULT);
        log.info("Publishing doc to elk... ");
    }

    SearchResponse findDocument(String key, String value) throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.termQuery(key, value));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(indexName);
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return searchResponse;
    }

    SearchResponse findNotExistField(String field) throws IOException {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(field)));
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices(indexName);
        searchRequest.source(sourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return searchResponse;
    }

    Boolean isUnique(String value) throws IOException {
        SearchResponse result = findDocument("md5", value);
        TotalHits number = result.getHits().getTotalHits();
        if (number.value < 1) {
            return Boolean.TRUE;
        } else {
            log.info("value: " + value + " not unique: " + number.value);
            return Boolean.FALSE;
        }
    }

    void updateDocument(String id, List<String> shingles) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest(indexName, id);
        XContentBuilder contentBuilder = XContentFactory.jsonBuilder();
        contentBuilder.startObject();
        contentBuilder.field("shingles", shingles);
        contentBuilder.endObject();

        updateRequest.doc(contentBuilder);
        UpdateResponse updateResponse = client.update(updateRequest, RequestOptions.DEFAULT);
        if (updateResponse.getResult() == UpdateResponse.Result.UPDATED) {
            log.info("Document updated successfully");
        } else if (updateResponse.getResult() == UpdateResponse.Result.NOOP) {
            log.error("No changes made to the document");
        }
    }

    SearchResponse getAllDocuments() throws IOException {
        SearchRequest searchRequest = new SearchRequest(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchSourceBuilder.size(1000);
        searchSourceBuilder.sort("_doc", SortOrder.ASC);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        return searchResponse;
    }
}
