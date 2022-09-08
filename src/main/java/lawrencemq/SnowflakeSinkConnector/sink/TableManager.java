package lawrencemq.SnowflakeSinkConnector.sink;


import lawrencemq.SnowflakeSinkConnector.sink.exceptions.TableAlterOrCreateException;
import lawrencemq.SnowflakeSinkConnector.sink.exceptions.TableDoesNotExistException;
import lawrencemq.SnowflakeSinkConnector.sql.Table;
import lawrencemq.SnowflakeSinkConnector.sql.TableDescription;
import lawrencemq.SnowflakeSinkConnector.sql.SnowflakeSql;
import org.apache.kafka.connect.data.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


final class TableManager {
    private static final Logger log = LoggerFactory.getLogger(TableManager.class);


    private final Table table;
    private final SnowflakeSinkConnectorConfig config;

    private Optional<TableDescription> maybeTableDescription;

    TableManager(SnowflakeSinkConnectorConfig config, Table destinationTable) {
        this.config = config;
        this.table = destinationTable;
        this.maybeTableDescription = Optional.empty();
    }

    boolean createOrAmendTable(Connection connection, KafkaFieldsMetadata kafkaFieldsMetadata) throws SQLException, TableAlterOrCreateException {
        if (tableNeedsCreating(connection)) {
            createTableWith(connection, kafkaFieldsMetadata);
        }
        return amendIfNecessary(connection, kafkaFieldsMetadata, config.maxRetries);
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
        Optional<TableDescription> desc = fetchTableDescription(connection);
        if (desc.isPresent()) {
            return desc;
        }
        return refreshTableDescription(connection);
    }

    void create(Connection connection, KafkaFieldsMetadata kafkaFieldsMetadata) throws SQLException, TableAlterOrCreateException {
        if (!config.autoCreate) {
            throw new TableAlterOrCreateException(
                    String.format("Table %s is missing and auto-creation is disabled", table)
            );
        }
        String sql = SnowflakeSql.buildCreateTableStatement(table, kafkaFieldsMetadata.getAllFields().values());
        log.info("Creating table with sql: {}", sql);
        SnowflakeSql.applyDdlStatement(connection, sql);
    }

    boolean amendIfNecessary(Connection connection, KafkaFieldsMetadata kafkaFieldsMetadata, int maxRetries) throws SQLException, TableAlterOrCreateException, TableDoesNotExistException {

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
            SnowflakeSql.applyDdlStatement(connection, amendTableQueries);
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

    private TableDescription getLatestTableDescription(Connection connection) throws SQLException {
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

    Set<Schema> findMissingFields(Collection<Schema> kafkaFields, Set<String> currentTableColumns) {
        Set<String> upperCaseColumns = currentTableColumns.stream()
                .map(String::toUpperCase)
                .collect(Collectors.toSet());

        return kafkaFields.stream()
                .filter(field -> !currentTableColumns.contains(field.name()) && !upperCaseColumns.contains(field.name().toUpperCase()))
                .collect(Collectors.toSet());
    }

    Optional<TableDescription> fetchTableDescription(Connection connection) throws SQLException {
        if (maybeTableDescription.isEmpty() && SnowflakeSql.tableExists(connection, table)) {
            Optional<TableDescription> dbTable = SnowflakeSql.describeTable(connection, table);
            if (dbTable.isPresent()) {
                log.info("Setting metadata for table {} to {}", table, dbTable.get());
                maybeTableDescription = dbTable;
            }
        }
        return maybeTableDescription;
    }

    Optional<TableDescription> refreshTableDescription(Connection connection) throws SQLException {
        Optional<TableDescription> dbTable = SnowflakeSql.describeTable(connection, table);
        if (dbTable.isPresent()) {
            log.info("Refreshing metadata for table {} to {}", table, dbTable);
            maybeTableDescription = dbTable;
        } else {
            log.warn("Failed to refresh metadata for table {}", table);
        }
        return maybeTableDescription;
    }

    public Table getTable() {
        return table;
    }
}