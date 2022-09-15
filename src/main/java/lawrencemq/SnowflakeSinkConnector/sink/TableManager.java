package lawrencemq.SnowflakeSinkConnector.sink;


import lawrencemq.SnowflakeSinkConnector.sink.exceptions.TableAlterOrCreateException;
import lawrencemq.SnowflakeSinkConnector.sink.exceptions.TableDoesNotExistException;
import lawrencemq.SnowflakeSinkConnector.sql.ColumnDescription;
import lawrencemq.SnowflakeSinkConnector.sql.Table;
import lawrencemq.SnowflakeSinkConnector.sql.TableDescription;
import lawrencemq.SnowflakeSinkConnector.sql.SnowflakeSql;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


class TableManager {
    private static final Logger log = LoggerFactory.getLogger(TableManager.class);
    private static final String[] TABLE_TYPES = new String[]{"TABLE"};


    private final Table table;
    private final SnowflakeSinkConnectorConfig config;

    private Optional<TableDescription> maybeTableDescription;

    TableManager(SnowflakeSinkConnectorConfig config, Table destinationTable) {
        this.config = config;
        this.table = destinationTable;
        this.maybeTableDescription = Optional.empty();
    }

    public Table getTable() {
        return table;
    }
    protected boolean tableExists(Connection connection, Table table) throws SQLException {
        log.info("Checking Snowflake - does {} exist?", table);
        try (ResultSet rs = connection.getMetaData().getTables(
                table.getDatabaseName(),
                table.getSchemaName(),
                table.getTableName(),
                TABLE_TYPES
        )) {
            boolean exists = rs.next();  // cursor should not be at end if exists
            log.info("Table {} {}", table, exists ? "exists" : "does not exist");
            return exists;
        }
    }

    private LinkedHashMap<String, ColumnDescription> describeColumns(Connection connection, Table table) throws SQLException {
        log.debug("Querying for column metadata for {}", table);

        LinkedHashMap<String, ColumnDescription> results = new LinkedHashMap<>();
        try (ResultSet rs = connection.getMetaData().getColumns(
                table.getDatabaseName(),
                table.getSchemaName(),
                table.getTableName(),
                null
        )) {
            while (rs.next()) {
                String columnName = rs.getString(4);
                int jdbcType = rs.getInt(5);
                String typeName = rs.getString(6);
                int nullableValue = rs.getInt(11);
                boolean nullability = nullableValue == DatabaseMetaData.columnNullable;

                results.put(columnName, new ColumnDescription(
                        columnName,
                        jdbcType,
                        typeName,
                        nullability
                ));
            }
            return results;
        }
    }

    private Optional<TableDescription> describeTable(Connection connection, Table table) throws SQLException {
        LinkedHashMap<String, ColumnDescription> columnDescriptions = describeColumns(connection, table);
        if (columnDescriptions.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(new TableDescription(table, columnDescriptions));
    }

    private void applyDdlStatement(Connection connection, String ddlStatement) throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.executeUpdate(ddlStatement);
        }
        try {
            connection.commit();
        } catch (Exception e) { // if anything unexpected happens, rollback
            try {
                connection.rollback();
            } catch (SQLException e2) {
                e.addSuppressed(e2);
            } finally {
                throw e;
            }
        }
    }

    boolean createOrAmendTable(Connection connection, KafkaFieldsMetadata kafkaFieldsMetadata) throws SQLException, TableAlterOrCreateException {
        createTableIfNecessary(connection, kafkaFieldsMetadata);
        return amendIfNecessary(connection, kafkaFieldsMetadata, config.maxRetries);
    }

    protected void createTableIfNecessary(Connection connection, KafkaFieldsMetadata kafkaFieldsMetadata) throws SQLException {
        if (tableNeedsCreating(connection)) {
            createTableWith(connection, kafkaFieldsMetadata);
        }
    }

    private void createTableWith(Connection connection, KafkaFieldsMetadata kafkaFieldsMetadata) throws SQLException {
        try {
            create(connection, kafkaFieldsMetadata);
        } catch (SQLException e) {
            log.warn("Create failed, will attempt amend if table already exists", e);
            try {
                Optional<TableDescription> newDesc = refreshTableDescription(connection);
                if (newDesc.isEmpty()) {
                    throw e;
                }
            } catch (SQLException e1) {
                log.warn(e1.getMessage());
                throw e1;
            }
        }
    }

    private boolean tableNeedsCreating(Connection connection) throws SQLException {
        return fetchTableDescription(connection).isEmpty();
    }

    Optional<TableDescription> getTableDescription(Connection connection) throws SQLException {
        // TODO THIS WHOLE PROCESS CAN BE SIMPLIFIED
        Optional<TableDescription> desc = fetchTableDescription(connection);
        if (desc.isPresent()) {
            return desc;
        }
        return refreshTableDescription(connection);
    }

    private void create(Connection connection, KafkaFieldsMetadata kafkaFieldsMetadata) throws SQLException, TableAlterOrCreateException {
        if (!config.autoCreate) {
            throw new TableAlterOrCreateException(
                    String.format("Table %s is missing and auto-creation is disabled", table)
            );
        }
        String sql = SnowflakeSql.buildCreateTableStatement(table, kafkaFieldsMetadata.getAllFields().values());
        log.info("Creating table with sql: {}", sql);
        applyDdlStatement(connection, sql);
    }

    protected boolean amendIfNecessary(Connection connection, KafkaFieldsMetadata kafkaFieldsMetadata, int maxRetries) throws SQLException, TableAlterOrCreateException, TableDoesNotExistException {

        TableDescription tableDesc = getLatestTableDescription(connection);

        Set<Schema> missingFields = findMissingFields(
                kafkaFieldsMetadata.getAllFields().values(),
                tableDesc.columnNames()
        );

        if (missingFields.isEmpty()) {
            return false;
        }

        if (!config.autoEvolve) {
            throw new TableAlterOrCreateException(String.format(
                    "%s is missing fields (%s) and auto-evolution is disabled",
                    table,
                    missingFields
            ));
        }

        ensureMissingFieldsConfigurations(missingFields);

        String amendTableQueries = SnowflakeSql.buildAlterTableStatement(table, missingFields);
        log.info("Amending table {}. Adding fields:{} with SQL: {}",
                table, missingFields, amendTableQueries
        );

        try {
            applyDdlStatement(connection, amendTableQueries);
        } catch (SQLException e) {
            if (maxRetries <= 0) {
                throw new TableAlterOrCreateException(
                        String.format(
                                "Failed to amend '%s' to add missing fields: %s",
                                table,
                                missingFields
                        ),
                        e
                );
            }
            log.warn("Amend failed, re-attempting", e);
            refreshTableDescription(connection);

            // Another task could have added the column(s) already - try again and validate
            return amendIfNecessary(
                    connection,
                    kafkaFieldsMetadata,
                    maxRetries - 1
            );
        }

        refreshTableDescription(connection);
        return true;
    }

    protected TableDescription getLatestTableDescription(Connection connection) throws SQLException {
        return fetchTableDescription(connection)
                .orElseThrow(() -> new TableDoesNotExistException(String.format("Unable to find table %s", table)));
    }

    private void ensureMissingFieldsConfigurations(Set<Schema> missingFields) {
        for (Schema missingField : missingFields) {
            if (!missingField.isOptional() && Objects.isNull(missingField.defaultValue())) {
                throw new TableAlterOrCreateException(String.format(
                        "Cannot ALTER %s. New field '%s' is not optional and has no default value",
                        table,
                        missingField
                ));
            }
        }
    }

    private Set<Schema> findMissingFields(Collection<Schema> kafkaFields, Set<String> currentTableColumns) {
        Set<String> upperCaseColumns = currentTableColumns.stream()
                .map(String::toUpperCase)
                .collect(Collectors.toSet());

        return kafkaFields.stream()
                .filter(field -> !upperCaseColumns.contains(field.name().toUpperCase()))
                .collect(Collectors.toSet());
    }

    private Optional<TableDescription> fetchTableDescription(Connection connection) throws SQLException {
        if (maybeTableDescription.isEmpty() && tableExists(connection, table)) {
            Optional<TableDescription> dbTable = describeTable(connection, table);
            if (dbTable.isPresent()) {
                log.info("Setting metadata for table {} to {}", table, dbTable.get());
                maybeTableDescription = dbTable;
            }
        }
        return maybeTableDescription;
    }

    protected Optional<TableDescription> refreshTableDescription(Connection connection) throws SQLException {
        Optional<TableDescription> dbTable = describeTable(connection, table);
        if (dbTable.isPresent()) {
            log.info("Refreshing metadata for table {} to {}", table, dbTable);
            maybeTableDescription = dbTable;
        } else {
            log.warn("Failed to refresh metadata for table {}", table);
        }
        return maybeTableDescription;
    }


}