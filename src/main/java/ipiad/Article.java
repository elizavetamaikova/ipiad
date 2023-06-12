package ipiad;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.codec.binary.Hex;
import org.json.JSONObject;

public class Article {
    String title;
    String author;
    String url;
    Date date;
    String content;
    String md5;

    public Article(String title, String author, String url, Date date, String content) throws NoSuchAlgorithmException {
        ObjectMapper mapper = new ObjectMapper();
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        this.title = title;
        this.author = author;
        this.url = url;
        this.date = date;
        this.content = content;
        md5.update(this.content.getBytes());
        String md5Hash = Hex.encodeHexString(md5.digest());
        this.md5 = md5Hash;
    }

    public JSONObject toJson() {
        JSONObject json = new JSONObject();
        json.put("title", title);
        json.put("author", author);
        json.put("url", url);
        json.put("date", date.toString());
        json.put("content", content);
        json.put("md5", md5);
        return json;
    }

    public Article(String jsonStr) throws ParseException {
        SimpleDateFormat formatter = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.ENGLISH);
        JSONObject json = new JSONObject(jsonStr);
        this.title = json.getString("title");
        this.author = json.getString("author");
        this.url = json.getString("url");
        this.date = formatter.parse(json.getString("date"));;
        this.content = json.getString("content");
        this.md5 = json.getString("md5");
    }

    public HashMap<String, String> toHashMap() throws NoSuchAlgorithmException {
        HashMap<String, String> article = new HashMap<String, String>();
        MessageDigest md5 = MessageDigest.getInstance("MD5");
        md5.update(this.content.getBytes());
        String md5Hash = Hex.encodeHexString(md5.digest());
        article.put("title", this.title);
        article.put("author", this.author);
        article.put("url", this.url);
        article.put("content", this.content);
        article.put("date", this.date.toString());
        article.put("md5", md5Hash);
        return article;
    }
}
