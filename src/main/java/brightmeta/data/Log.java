package brightmeta.data;

/**
 * Created by John on 6/11/17.
 */
public class Log {
    private String hostId;
    private String visitorIP;

    public Log(String host, String visitorIP) {
        this.hostId = host;
        this.visitorIP = visitorIP;
    }

    public String getHostId() {
        return hostId;
    }

    public String getVisitorIP() {
        return visitorIP;
    }

    @Override
    public String toString() {
        return hostId + "," + visitorIP;
    }
}
