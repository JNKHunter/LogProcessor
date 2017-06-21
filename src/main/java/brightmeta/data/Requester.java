package brightmeta.data;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

public class Requester {

    @SerializedName("ip")
    @Expose
    private String ip;
    @SerializedName("count")
    @Expose
    private Long count;

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

}