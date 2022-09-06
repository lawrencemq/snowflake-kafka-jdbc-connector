package lawrencemq.SnowflakeSinkConnector.sql;

import org.junit.jupiter.api.Test;

import static lawrencemq.SnowflakeSinkConnector.sql.QueryUtils.bytesToHex;
import static org.junit.jupiter.api.Assertions.assertEquals;


class QueryUtilsTest {

    @Test
    void testBytesToHex() {
        assertEquals(bytesToHex("HelloWorld!!!123#@$^%@#$%^".getBytes()), "48656C6C6F576F726C642121213132332340245E25402324255E");
    }
}