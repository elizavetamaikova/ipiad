package ipiad;

import java.util.Date;
import org.json.JSONObject;

public class Article {
    String title;
    String author;
    String url;
    Date date;
    String content;

    public Article(String title, String author, String url, Date date, String content) {
        this.title = title;
        this.author = author;
        this.url = url;
        this.date = date;
        this.content = content;
    }

    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        json.put("title", title);
        json.put("author", author);
        json.put("url", url);
        json.put("date", date);
        json.put("content", content);
        return json;
    }

}
