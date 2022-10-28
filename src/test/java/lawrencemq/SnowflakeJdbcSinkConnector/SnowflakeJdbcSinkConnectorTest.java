package lawrencemq.SnowflakeJdbcSinkConnector;

import lawrencemq.SnowflakeJdbcSinkConnector.sink.*;
import org.junit.jupiter.api.*;

import java.util.*;

import static lawrencemq.SnowflakeJdbcSinkConnector.TestData.DEFAULT_CONFIGS;
import static org.junit.jupiter.api.Assertions.*;

class SnowflakeJdbcSinkConnectorTest {

    SnowflakeJdbcSinkConnector connector;

    @BeforeEach
    void setupConnector(){
        connector = new SnowflakeJdbcSinkConnector();
    }

    @Test
    void taskClass(){
        assertEquals(connector.taskClass(), SnowflakeJdbcSinkTask.class);
    }

    @Test
    void start() {
        assertDoesNotThrow(() -> connector.start(DEFAULT_CONFIGS));
    }

    @Test
    void taskConfigs() {
        connector.start(DEFAULT_CONFIGS);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(10);
        assertEquals(taskConfigs.size(), 10);
        for(Map<String, String> config : taskConfigs){
            assertEquals(config, DEFAULT_CONFIGS);
        }
    }


    @Test
    void stop() {
        assertDoesNotThrow(() -> connector.stop());
    }

    @Test
    void validate() {
        assertNotNull(connector.validate(DEFAULT_CONFIGS));
    }

    @Test
    void config() {
        assertEquals(connector.config(), SnowflakeJdbcSinkConnectorConfig.CONFIG_DEF);
    }

    @Test
    void version(){
        assertEquals(connector.version(), "0.2.0-SNAPSHOT");
    }
}