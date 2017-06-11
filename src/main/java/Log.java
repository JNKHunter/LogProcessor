/**
 * Created by John on 6/11/17.
 */
public class Log {
    private String host;
    private String visitorIP;

    public Log(String host, String visitorIP) {
        this.host = host;
        this.visitorIP = visitorIP;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getVisitorIP() {
        return visitorIP;
    }

    public void setVisitorIP(String visitorIP) {
        this.visitorIP = visitorIP;
    }
}
