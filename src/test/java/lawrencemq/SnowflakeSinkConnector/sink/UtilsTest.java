package lawrencemq.SnowflakeSinkConnector.sink;

import org.junit.jupiter.api.Test;

import static lawrencemq.SnowflakeSinkConnector.sink.Utils.bytesToHex;
import static org.junit.jupiter.api.Assertions.*;

class UtilsTest {

    @Test
    void testBytesToHex() {
        assertEquals(bytesToHex("HelloWorld!!!123#@$^%@#$%^".getBytes()), "48656C6C6F576F726C642121213132332340245E25402324255E");
    }
}