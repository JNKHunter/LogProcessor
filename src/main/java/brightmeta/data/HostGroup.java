package brightmeta.data;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.frequency.CountMinSketch;

import java.util.Random;

/**
 * Created by John on 6/19/17.
 */

public class HostGroup {
    private String hostId;
    private long requestCount;
    private boolean isDDos;
    private CountMinSketch ipCounts;
    private HyperLogLog requesterSet;

    public HostGroup() {
        ipCounts = new CountMinSketch(0.001, 0.99, new Random().nextInt(100));
        requesterSet = new HyperLogLog(0.03);
    }

    public void addRequest() {
        requestCount += 1;
    }

    public String getHostId() {
        return hostId;
    }

    public void setHostId(String hostId) {
        this.hostId = hostId;
    }

    public long getRequestCount() {
        return requestCount;
    }

    public void setRequestCount(long requestCount) {
        this.requestCount = requestCount;
    }

    public boolean isDDos() {
        return isDDos;
    }

    public void setDDos(boolean DDos) {
        isDDos = DDos;
    }

    public void addIp(String ip) {
        addRequest();
        ipCounts.add(ip, 1);
        requesterSet.offer(ip);
    }

    public long getNumberOfRequesters() {
        return requesterSet.cardinality();
    }

}
