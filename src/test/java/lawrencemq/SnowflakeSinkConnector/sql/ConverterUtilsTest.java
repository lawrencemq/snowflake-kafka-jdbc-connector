package lawrencemq.SnowflakeSinkConnector.sql;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static lawrencemq.SnowflakeSinkConnector.sql.ConverterUtils.convertToJSON;
import static org.junit.jupiter.api.Assertions.*;

class ConverterUtilsTest {

    @Test
    void testConvertToJSON() {
        assertEquals(convertToJSON("hello"), "\"hello\"");
        assertEquals(convertToJSON(1), "1");
        assertEquals(convertToJSON(1.2), "1.2");
        assertEquals(convertToJSON(List.of(1, 2, 3, 4, 5)), "[1,2,3,4,5]");

        // Map keys can be rearranged during JSON-ification.
        String convertedMap = convertToJSON(Map.of(1, 2, 3, 4));
        assertTrue(convertedMap.equals("{\"1\":2,\"3\":4}") || convertedMap.equals("{\"3\":4,\"1\":2}"));
    }
}