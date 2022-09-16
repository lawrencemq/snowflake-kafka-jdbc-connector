package lawrencemq.SnowflakeSinkConnector.sink;

import lawrencemq.SnowflakeSinkConnector.sink.exceptions.InvalidColumnsError;
import lawrencemq.SnowflakeSinkConnector.sink.exceptions.TableAlterOrCreateException;
import lawrencemq.SnowflakeSinkConnector.sql.Table;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static lawrencemq.SnowflakeSinkConnector.sink.SnowflakeSinkConnectorConfig.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class TableManagerTest {


    private final static String DATABASE = "db1";
    private final static String SCHEMA = "schema2";
    private final static String TABLE_NAME = "table3";

    private final static Table TABLE = new Table(DATABASE, SCHEMA, TABLE_NAME);


    private static final Schema KEY_SCHEMA = SchemaBuilder.struct()
            .name("keySchema")
            .field("id", SchemaBuilder.string().name("id").build())
            .build();
    private static final Schema VALUE_SCHEMA = SchemaBuilder.struct()
            .name("valueSchema")
            .field("string", SchemaBuilder.string().name("string").build())
            .field("int16maybe", SchemaBuilder.int16().optional().name("int16maybe").build())
            .field("trueMaybe", SchemaBuilder.bool().optional().name("trueMaybe").build())
            .build();

    private static Map<String, Schema> getMapOf(Schema... allSchemas) {
        Map<String, Schema> fieldNameToSchemaMap = new LinkedHashMap<>();
        Arrays.stream(allSchemas)
                .map(Schema::fields)
                .flatMap(Collection::stream)
                .forEach(field -> fieldNameToSchemaMap.put(field.name(), field.schema()));
        return fieldNameToSchemaMap;
    }

    private static Set<String> getFieldNamesFor(Schema schema) {
        Set<String> fieldNames = new LinkedHashSet<>();
        schema.fields()
                .stream()
                .map(Field::name)
                .forEach(fieldNames::add);
        return fieldNames;
    }

    private static SnowflakeSinkConnectorConfig genConfig() {
        return genConfig(Map.of());
    }

    private static SnowflakeSinkConnectorConfig genConfig(Map<?, ?> properties) {
        Map<?, ?> defaultConfigs = Map.of(
                SNOWFLAKE_USER_NAME, "testUser",
                SNOWFLAKE_PASSPHRASE, "butterCup123!",
                SNOWFLAKE_ACCOUNT, "123456789",
                SNOWFLAKE_WAREHOUSE, "testWH",
                SNOWFLAKE_ROLE, "defaultRole",
                SNOWFLAKE_DB, DATABASE,
                SNOWFLAKE_SCHEMA, SCHEMA,
                SNOWFLAKE_TABLE, TABLE_NAME
        );


        Map<?, ?> finalConfigs = Stream.of(defaultConfigs, properties)
                .flatMap(map -> map.entrySet().stream())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        Map.Entry::getValue
                ));
        return new SnowflakeSinkConnectorConfig(finalConfigs);
    }

    @Test
    void createTableFailsAutoCreateIsFalse() {
        Connection connection = mock(Connection.class);
        SnowflakeSinkConnectorConfig config = genConfig(Map.of(AUTO_CREATE, false));
        TableManager tableManager = new TableManager(config, TABLE) {
            @Override
            protected boolean tableExists(Connection connection, Table table) {
                return false;
            }
        };

        assertThrows(TableAlterOrCreateException.class,
                () -> tableManager.createOrAmendTable(connection,
                        new KafkaFieldsMetadata(getFieldNamesFor(KEY_SCHEMA),
                                getFieldNamesFor(VALUE_SCHEMA),
                                getMapOf(KEY_SCHEMA, VALUE_SCHEMA)
                        )
                )
        );
    }

    @Test
    void createTableIfNecessaryCreatesNewTable() throws SQLException {
        Connection connection = mock(Connection.class);
        SnowflakeSinkConnectorConfig config = genConfig();
        TableManager tableManager = new TableManager(config, TABLE) {
            @Override
            protected boolean tableExists(Connection connection, Table table) {
                return false;
            }

        };

        Statement statement = mock(Statement.class);
        when(connection.createStatement()).thenReturn(statement);

        tableManager.createTableIfNecessary(connection,
                new KafkaFieldsMetadata(getFieldNamesFor(KEY_SCHEMA),
                        getFieldNamesFor(VALUE_SCHEMA),
                        getMapOf(KEY_SCHEMA, VALUE_SCHEMA)
                )
        );

        String expectedCreateStatement = "CREATE TABLE \"DB1\".\"SCHEMA2\".\"TABLE3\" (\n" +
                "ID TEXT NOT NULL,\n" +
                "STRING TEXT NOT NULL,\n" +
                "INT16MAYBE NUMBER,\n" +
                "TRUEMAYBE BOOLEAN)";
        verify(statement).executeUpdate(expectedCreateStatement);
    }

    @Test
    void amendTableFailsAutoEvolveIsFalse() {
        Connection connection = mock(Connection.class);
        LinkedHashSet<String> description = new LinkedHashSet<>();
        description.add("string");

        SnowflakeSinkConnectorConfig config = genConfig(Map.of(AUTO_EVOLVE, false));
        TableManager tableManager = new TableManager(config, TABLE) {
            @Override
            protected LinkedHashSet<String> getLatestTableColumns(Connection connection) {
                return description;
            }
        };

        assertThrows(TableAlterOrCreateException.class,
                () -> tableManager.amendIfNecessary(connection,
                        new KafkaFieldsMetadata(getFieldNamesFor(KEY_SCHEMA),
                                getFieldNamesFor(VALUE_SCHEMA),
                                getMapOf(KEY_SCHEMA, VALUE_SCHEMA)
                        ),
                        5
                ),
                "Should throw error if evolution needed but disallowed by config"
        );
    }

    @Test
    void amendDoesNotAmendWhenSchemaIsUnchanged() throws SQLException {

        Set<String> allColumnNames = getFieldNamesFor(VALUE_SCHEMA);
        allColumnNames.addAll(getFieldNamesFor(KEY_SCHEMA));

        Connection connection = mock(Connection.class);
        LinkedHashSet<String> description = new LinkedHashSet<>(allColumnNames);

        SnowflakeSinkConnectorConfig config = genConfig();
        TableManager tableManager = new TableManager(config, TABLE) {
            @Override
            protected LinkedHashSet<String> getLatestTableColumns(Connection connection) {
                return description;
            }
        };

        assertFalse(
                tableManager.amendIfNecessary(connection,
                        new KafkaFieldsMetadata(getFieldNamesFor(KEY_SCHEMA),
                                getFieldNamesFor(VALUE_SCHEMA),
                                getMapOf(KEY_SCHEMA, VALUE_SCHEMA)
                        ),
                        5
                )
        );
    }

    @Test
    void amendFailsWithNewNonNullFieldWithNoDefault() {
        Connection connection = mock(Connection.class);
        LinkedHashSet<String> description = new LinkedHashSet<>();
        description.add("int16maybe");

        SnowflakeSinkConnectorConfig config = genConfig();
        TableManager tableManager = new TableManager(config, TABLE) {
            @Override
            protected LinkedHashSet<String> getLatestTableColumns(Connection connection) {
                return description;
            }
        };

        assertThrows(TableAlterOrCreateException.class,
                () -> tableManager.amendIfNecessary(connection,
                        new KafkaFieldsMetadata(getFieldNamesFor(KEY_SCHEMA),
                                getFieldNamesFor(VALUE_SCHEMA),
                                getMapOf(KEY_SCHEMA, VALUE_SCHEMA)
                        ),
                        5
                ),
                "Should throw error when non-optional field is added but has not default"
        );
    }

    @Test
    void createOrAmendTableAmendsTable() throws SQLException {
        Connection connection = mock(Connection.class);
        LinkedHashSet<String> description = new LinkedHashSet<>();
        description.add("id");
        description.add("string");

        Statement statement = mock(Statement.class);
        when(connection.createStatement()).thenReturn(statement);

        SnowflakeSinkConnectorConfig config = genConfig();
        TableManager tableManager = new TableManager(config, TABLE) {
            @Override
            protected boolean tableExists(Connection connection, Table table){
                return true;
            }

            @Override
            protected LinkedHashSet<String> describeTable(Connection connection, Table table){
                return description;
            }
        };

        assertTrue(tableManager.amendIfNecessary(connection,
                new KafkaFieldsMetadata(getFieldNamesFor(KEY_SCHEMA),
                        getFieldNamesFor(VALUE_SCHEMA),
                        getMapOf(KEY_SCHEMA, VALUE_SCHEMA)
                ),
                5
        ));

        String expectedAlterStatement = "ALTER TABLE \"DB1\".\"SCHEMA2\".\"TABLE3\" \n" +
                "ADD INT16MAYBE NUMBER,\n" +
                "ADD TRUEMAYBE BOOLEAN";
        verify(statement).executeUpdate(expectedAlterStatement);
        verify(connection).commit();
    }

    @Test
    void refreshTableChecksForEmptyTableThatExists() throws SQLException {
        Connection connection = mock(Connection.class);
        SnowflakeSinkConnectorConfig config = genConfig();
        TableManager tableManager = new TableManager(config, TABLE) {
            @Override
            protected boolean tableExists(Connection connection, Table table){
                return true;
            }

            @Override
            protected LinkedHashSet<String> describeTable(Connection connection, Table table){
                return new LinkedHashSet<>();
            }
        };

        assertThrows(InvalidColumnsError.class,
                () -> tableManager.refreshTableDescription(connection),
                "Should error if table is found to have no columns.");

    }


}