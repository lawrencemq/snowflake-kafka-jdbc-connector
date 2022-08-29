package lawrencemq.SnowflakeSinkConnector.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class Utils {

    private static final Logger log = LoggerFactory.getLogger(Utils.class);



    private static final byte[] HEX_ARRAY = "0123456789ABCDEF".getBytes(StandardCharsets.US_ASCII);
    public static String bytesToHex(byte[] bytes) {
        // https://stackoverflow.com/questions/9655181/how-to-convert-a-byte-array-to-a-hex-string-in-java
        byte[] hexChars = new byte[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars, StandardCharsets.UTF_8);
    }

    static String getVersion(){
        String path = "/snowflake-kafka-jdbc-connector.properties";
        String defaultVersion = "unknown";
        try (InputStream stream = Utils.class.getResourceAsStream(path)) {
            Properties props = new Properties();
            props.load(stream);
            return props.getProperty("version", defaultVersion).trim();
        } catch (Exception e) {
            log.warn("Error while loading properties:", e);
            return defaultVersion;
        }
    }
}
