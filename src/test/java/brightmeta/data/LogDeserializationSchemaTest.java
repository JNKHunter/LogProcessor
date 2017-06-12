package brightmeta.data;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by John on 6/11/17.
 */
public class LogDeserializationSchemaTest {
    private LogDeserializationSchema schema;

    @Before
    public void setUp() throws Exception {
        schema = new LogDeserializationSchema();
    }

    @Test
    public void testDeserialize() throws Exception {
        String host = "1";
        String visitor = "192.168.1.1";
        String logString = host + "," + visitor;
        byte[] message = logString.getBytes();


        Log log = schema.deserialize(message);
        assertEquals(log.getHostId(), host);
        assertEquals(log.getVisitorIP(), visitor);

    }

}