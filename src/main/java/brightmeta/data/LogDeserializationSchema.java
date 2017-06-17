package brightmeta.data;

import brightmeta.data.Log;
import org.apache.flink.streaming.util.serialization.AbstractDeserializationSchema;

import java.io.IOException;

/**
 * Created by John on 6/11/17.
 */
public class LogDeserializationSchema extends AbstractDeserializationSchema<Log> {

    @Override
    public Log deserialize(byte[] message) throws IOException {
        String[] keyVal = (new String(message)).split(",");
        String[] hostAndIp = keyVal[1].split("|");
        return new Log(keyVal[0], hostAndIp[0], hostAndIp[1]);
    }
}
