package brightmeta.data;

import scala.Tuple2;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;

/**
 * Created by John on 6/11/17.
 */
public class Log {

    private String machineKey;
    private String hostId;
    private String visitorIP;

    public Log(String host, String visitorIP) throws SocketException, UnknownHostException {
        this.hostId = host;
        this.visitorIP = visitorIP;
        this.machineKey = getAddress();
    }

    public String getHostId() {
        return hostId;
    }

    public String getVisitorIP() {
        return visitorIP;
    }

    public String getMachineKey() { return machineKey; }

    public Tuple2<String, String> getKey() {
        return new Tuple2<>(getHostId(), machineKey);
    }
    

    @Override
    public String toString() {
        return hostId + "," + visitorIP;
    }

    private String getAddress() throws SocketException, UnknownHostException {
        return InetAddress.getLocalHost().toString();
    }
}
