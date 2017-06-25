package brightmeta.data;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by John on 6/25/17.
 */
public class HostGroupTest {

    private HostGroup hostGroup;

    @Before
    public void setup() {
        hostGroup = new HostGroup();
    }

    @Test
    public void testAddRequestCount() {
        hostGroup.addIp("192.168.1.1");
        assertEquals(1, hostGroup.getRequestCount());
    }

}