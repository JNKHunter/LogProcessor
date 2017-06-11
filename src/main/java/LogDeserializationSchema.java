import org.apache.flink.streaming.util.serialization.AbstractDeserializationSchema;

import java.io.IOException;

/**
 * Created by John on 6/11/17.
 */
public class LogDeserializationSchema extends AbstractDeserializationSchema<LogDeserializationSchema> {
    @Override
    public boolean isEndOfStream(LogDeserializationSchema nextElement) {
        return super.isEndOfStream(nextElement);
    }

    @Override
    public LogDeserializationSchema deserialize(byte[] message) throws IOException {

        return null;
    }
}
