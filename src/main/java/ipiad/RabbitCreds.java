package ipiad;

    public class RabbitCreds {
    public String username;
    public String password;
    public String virtualHost;
    public String host;
    public int port;

    public RabbitCreds(String username,
                       String password,
                       String virtualHost,
                       String host,
                       int port){
        this.username = username;
        this.password = password;
        this.virtualHost = virtualHost;
        this.host = host;
        this.port = port;
    }

}
