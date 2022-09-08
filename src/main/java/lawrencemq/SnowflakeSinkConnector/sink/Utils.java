package lawrencemq.SnowflakeSinkConnector.sink;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.Properties;

public final class Utils {

    private static final Logger log = LoggerFactory.getLogger(Utils.class);

    public static String getVersion(){
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
