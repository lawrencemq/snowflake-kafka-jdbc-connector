package lawrencemq.SnowflakeSinkConnector.sink;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class SnowflakeSinkConnectorConfigTest {

    Properties createDefaultConfig(){
        Properties props = new Properties();
        props.put("snowflake.username", "testName");
        props.put("snowflake.account", "12345");
        props.put("snowflake.warehouse", "warehouse1");
        props.put("snowflake.role", "kafka_connect_system_role");
        props.put("snowflake.database", "kafka_connect");
        props.put("snowflake.schema", "landing");
        props.put("snowflake.table", "test_table1");
        props.put("snowflake.private.key.filename", "test_filename");
        props.put("snowflake.passphrase", "thegooseisloose123!");
        props.put("snowflake.max.retries", "5");
        props.put("snowflake.retry.backoffMs", "5000");
        props.put("batch.size", "10000");
        props.put("auto.create", "true");
        props.put("auto.evolve", "true");
        return props;
    }

    @Test
    void configDef() {
        Set<String> allConfigNames = SnowflakeSinkConnectorConfig.configDef().names();
        Set<String> expectedConfigNames = Set.of(
                "snowflake.username",
                "snowflake.account",
                "snowflake.warehouse",
                "snowflake.role",
                "snowflake.database",
                "snowflake.schema",
                "snowflake.table",
                "snowflake.private.key.filename",
                "snowflake.passphrase",
                "snowflake.max.retries",
                "snowflake.retry.backoffMs",
                "batch.size",
                "auto.create",
                "auto.evolve"
        );

        assertEquals(allConfigNames, expectedConfigNames);

    }

    @Test
    void shouldCreateValidConfig(){
        SnowflakeSinkConnectorConfig config = new SnowflakeSinkConnectorConfig(createDefaultConfig());
        assertNotNull(config);
    }

    @Test
    void shouldRequireSnowflakeUsername(){
        Properties propsWithoutUsername = createDefaultConfig();
        propsWithoutUsername.remove("snowflake.username");
        ensureThrowsConfigErrorWithMessage(propsWithoutUsername, "Missing required configuration \"snowflake.username\"");
    }

    @Test
    void shouldRequireSnowflakeAccount(){
        Properties propsWithoutAccount = createDefaultConfig();
        propsWithoutAccount.remove("snowflake.account");
        ensureThrowsConfigErrorWithMessage(propsWithoutAccount, "Missing required configuration \"snowflake.account\"");
    }

    @Test
    void shouldRequireSnowflakeWarehouse(){
        Properties propsWithoutWarehouse = createDefaultConfig();
        propsWithoutWarehouse.remove("snowflake.warehouse");
        ensureThrowsConfigErrorWithMessage(propsWithoutWarehouse, "Missing required configuration \"snowflake.warehouse\"");
    }

    @Test
    void shouldRequireSnowflakeRole(){
        Properties propsWithoutRole = createDefaultConfig();
        propsWithoutRole.remove("snowflake.role");
        ensureThrowsConfigErrorWithMessage(propsWithoutRole, "Missing required configuration \"snowflake.role\"");
    }

    @Test
    void shouldRequireSnowflakeDatabase(){
        Properties propsWithoutDb = createDefaultConfig();
        propsWithoutDb.remove("snowflake.database");
        ensureThrowsConfigErrorWithMessage(propsWithoutDb, "Missing required configuration \"snowflake.database\"");
    }

    @Test
    void shouldRequireSnowflakeSchema(){
        Properties propsWithoutSchema = createDefaultConfig();
        propsWithoutSchema.remove("snowflake.schema");
        ensureThrowsConfigErrorWithMessage(propsWithoutSchema, "Missing required configuration \"snowflake.schema\"");
    }

    @Test
    void shouldRequireSnowflakeTable(){
        Properties propsWithoutTable = createDefaultConfig();
        propsWithoutTable.remove("snowflake.table");
        ensureThrowsConfigErrorWithMessage(propsWithoutTable, "Missing required configuration \"snowflake.table\"");
    }

    @Test
    void ensureDefaultArgumentsAreSet(){
        Properties props = new Properties();
        props.put("snowflake.username", "testName");
        props.put("snowflake.account", "12345");
        props.put("snowflake.warehouse", "warehouse1");
        props.put("snowflake.role", "kafka_connect_system_role");
        props.put("snowflake.database", "kafka_connect");
        props.put("snowflake.schema", "landing");
        props.put("snowflake.table", "test_table1");
        props.put("snowflake.private.key.filename", "test_file.abc");

        SnowflakeSinkConnectorConfig config = new SnowflakeSinkConnectorConfig(props);

        assertEquals(config.maxRetries, 3);
        assertEquals(config.retryBackoffMs, 5_000);
        assertEquals(config.batchSize, 3_000);
        assertTrue(config.autoCreate);
        assertTrue(config.autoEvolve);

    }



    void ensureThrowsConfigErrorWithMessage(Properties props, String expectedMessage){
        ConfigException thrown = assertThrows(
                ConfigException.class,
                () ->  new SnowflakeSinkConnectorConfig(props),
                "Expected config without required fields to throw error"
        );
        String message = thrown.getMessage();
        if(!thrown.getMessage().contains(expectedMessage)){
            fail("Message \""+expectedMessage+"\" not found in output \""+message+"\"", thrown);
        }

//        assertTrue(thrown.getMessage().contains(expectedMessage), "Expected error message did not appear.");
    }

}