package lawrencemq.SnowflakeSinkConnector.sink;


import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class DateTimeUtils {
    private final static TimeZone TIME_ZONE = TimeZone.getTimeZone("UTC");

    private final static SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    private final static SimpleDateFormat TIME_FORMAT = new SimpleDateFormat("HH:mm:ss.SSS");
    private final static SimpleDateFormat TIMESTAMP_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    static {
        DATE_FORMAT.setTimeZone(TIME_ZONE);
        TIME_FORMAT.setTimeZone(TIME_ZONE);
        TIMESTAMP_FORMAT.setTimeZone(TIME_ZONE);
    }
    public static Calendar getTimeZoneCalendar() {
        return new GregorianCalendar(TIME_ZONE);
    }

    /**
     * Formats days in YYYY-MM-DD using UTC. See more in Snowflake documentation:
     * https://docs.snowflake.com/en/sql-reference/data-types-datetime.html#date
     * @param date
     * @return
     */
    public static String formatDate(Date date) {
            return DATE_FORMAT.format(date);
    }

    /**
     * Formats time in HH:mm:ss.SSSSSSSSS using UTC.
     * See more in Snowflake documentation:
     * https://docs.snowflake.com/en/sql-reference/data-types-datetime.html#time
     * @param date
     * @return
     */
    public static String formatTime(Date date) {
            return TIME_FORMAT.format(date);
    }

    /**
     * Formats timestamp_tz in YYYY-MM-DD HH:mm:ss.SSSSSSSSS using UTC.
     * See more in Snowflake documentation:
     * https://docs.snowflake.com/en/sql-reference/data-types-datetime.html#timestamp-ltz-timestamp-ntz-timestamp-tz
     * @param date
     * @return
     */
    public static String formatTimestamp(Date date) {
            return TIMESTAMP_FORMAT.format(date);
    }

}