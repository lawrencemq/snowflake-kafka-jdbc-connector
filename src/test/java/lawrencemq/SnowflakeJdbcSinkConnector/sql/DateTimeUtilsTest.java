package lawrencemq.SnowflakeJdbcSinkConnector.sql;

import org.junit.jupiter.api.Test;

import java.util.Date;

import static lawrencemq.SnowflakeJdbcSinkConnector.sql.DateTimeUtils.*;
import static org.junit.jupiter.api.Assertions.*;

class DateTimeUtilsTest {

    final static long EPOCH_TIME = 1662428765123L; // Tue, 06 Sep 2022 01:46:05 GMT
    @Test
    void testFormatDate() {
        assertEquals(formatDate(new Date(EPOCH_TIME)), "2022-09-06");
    }

    @Test
    void testFormatTime() {
        assertEquals(formatTime(new Date(EPOCH_TIME)), "01:46:05.123");
    }

    @Test
    void testFormatTimestamp() {
        assertEquals(formatTimestamp(new Date(EPOCH_TIME)), "2022-09-06 01:46:05.123");
    }
}