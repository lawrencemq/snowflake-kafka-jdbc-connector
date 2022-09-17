package lawrencemq.SnowflakeSinkConnector.sink;


import lawrencemq.SnowflakeSinkConnector.sink.exceptions.*;
import lawrencemq.SnowflakeSinkConnector.sql.Table;
import lawrencemq.SnowflakeSinkConnector.sql.SnowflakeSql;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;


class TableManager {
    private static final Logger log = LoggerFactory.getLogger(TableManager.class);
    private static final String[] TABLE_TYPES = new String[]{"TABLE"};


    private final Table table;
    private final SnowflakeSinkConnectorConfig config;

    private LinkedHashSet<String> latestTableDescription;

    TableManager(SnowflakeSinkConnectorConfig config, Table destinationTable) {
        this.config = config;
        this.table = destinationTable;
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


    protected LinkedHashSet<String> describeTable(Connection connection, Table table) throws SQLException {
        log.debug("Querying for column metadata for {}", table);

        LinkedHashSet<String> columnDescriptions = new LinkedHashSet<>();
        try (ResultSet rs = connection.getMetaData().getColumns(
                table.getDatabaseName(),
                table.getSchemaName(),
                table.getTableName(),
                null
        )) {
            while (rs.next()) {
                String columnName = rs.getString(4);
//                int jdbcType = rs.getInt(5);
//                String typeName = rs.getString(6);
//                int nullableValue = rs.getInt(11);
//                boolean nullability = nullableValue == DatabaseMetaData.columnNullable;

                columnDescriptions.add(columnName);
            }
        }
        return columnDescriptions;
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
                LinkedHashSet<String> newDesc = refreshTableDescription(connection);
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
        try{
            return getLatestTableColumns(connection).isEmpty();
        }catch (TableDoesNotExistException e){
            return true;
        }
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

        LinkedHashSet<String> tableDesc = getLatestTableColumns(connection);

        List<Schema> missingFields = findMissingFields(
                kafkaFieldsMetadata.getAllFields().values(),
                tableDesc
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

    private void ensureMissingFieldsConfigurations(List<Schema> missingFields) {
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

    private List<Schema> findMissingFields(Collection<Schema> kafkaFields, Set<String> currentTableColumns) {
        Set<String> upperCaseTableColumns = currentTableColumns.stream()
                .map(String::toUpperCase)
                .collect(Collectors.toSet());

        return kafkaFields.stream()
                .filter(field -> !upperCaseTableColumns.contains(field.name().toUpperCase()))
                .collect(Collectors.toList());
    }

    protected LinkedHashSet<String> getLatestTableColumns(Connection connection) throws SQLException {
        if (Objects.isNull(latestTableDescription) || latestTableDescription.isEmpty()) {
            return refreshTableDescription(connection);
        }
        return latestTableDescription;
    }

    protected LinkedHashSet<String> refreshTableDescription(Connection connection) throws SQLException {

        if (!tableExists(connection, table)) {
            throw new TableDoesNotExistException(String.format("Unable to find table %s", table));
        }

        LinkedHashSet<String> updatedDescription = describeTable(connection, table);
        if (updatedDescription.isEmpty()) {
            throw new InvalidColumnsError("Failed to refresh metadata for table " + table);
        } else {
            log.info("Setting metadata for table {} to {}", table, updatedDescription);
            latestTableDescription = updatedDescription;

        }
        return latestTableDescription;
    }


}