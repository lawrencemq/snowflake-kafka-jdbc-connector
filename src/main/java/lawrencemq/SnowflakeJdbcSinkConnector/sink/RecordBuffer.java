package lawrencemq.SnowflakeJdbcSinkConnector.sink;

import lawrencemq.SnowflakeJdbcSinkConnector.sink.exceptions.TableAlterOrCreateException;
import lawrencemq.SnowflakeJdbcSinkConnector.sink.exceptions.TableDoesNotExistException;
import lawrencemq.SnowflakeJdbcSinkConnector.sql.QueryStatementBinder;
import lawrencemq.SnowflakeJdbcSinkConnector.sql.SnowflakeSql;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.BatchUpdateException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

/**
 * <a href="https://docs.snowflake.com/en/user-guide/jdbc-using.html#batch-inserts">See best practices for batch inserts here.</a>
 */
public class RecordBuffer {
    private static final Logger log = LoggerFactory.getLogger(RecordBuffer.class);

    private final SnowflakeJdbcSinkConnectorConfig config;
    private final TableManager tableManager;
    private final Connection connection;

    private List<SinkRecord> records = new ArrayList<>();
    private Schema keySchema;
    private Schema valueSchema;
    private PreparedStatement insertPreparedStatement;
    private QueryStatementBinder insertStatementBinder;

    public RecordBuffer(SnowflakeJdbcSinkConnectorConfig config, TableManager tableManager, Connection connection) {
        this.config = config;
        this.tableManager = tableManager;
        this.connection = connection;
    }

    private boolean hasSchemaChanged(SinkRecord record) {
        boolean changed = false;
        if (!Objects.equals(keySchema, record.keySchema())) {
            keySchema = record.keySchema();
            changed = true;
        }

        if (!Objects.equals(valueSchema, record.valueSchema())) {
            valueSchema = record.valueSchema();
            changed = true;
        }
        return changed;
    }

    public List<SinkRecord> addAll(Collection<SinkRecord> records) throws SQLException, TableAlterOrCreateException, TableDoesNotExistException {
        List<SinkRecord> flushed = new ArrayList<>();
        for (SinkRecord record : records) {
            flushed.addAll(add(record));
        }
        return flushed;
    }

    protected List<SinkRecord> add(SinkRecord record) throws SQLException, TableAlterOrCreateException, TableDoesNotExistException {
        RecordValidator.validate(record);
        List<SinkRecord> flushed = new ArrayList<>();

        if (hasSchemaChanged(record) || isNull(insertStatementBinder)) {
            log.debug("Creating a new statement and statement binder");

            // pushing all messages with old schema and using new schema
            flushed.addAll(flush());

            // re-initialize everything that depends on the record schema
            TopicSchemas topicSchemas = new TopicSchemas(
                    record.keySchema(),
                    record.valueSchema()
            );
            KafkaFieldsMetadata kafkaFieldsMetadata = KafkaFieldsMetadata.from(topicSchemas);
            tableManager.createOrAmendTable(
                    connection,
                    kafkaFieldsMetadata
            );
            String insertSql = SnowflakeSql.buildInsertStatement(tableManager.getTable(), kafkaFieldsMetadata.getAllFields());
            log.debug("sql: {}  meta: {}", insertSql, kafkaFieldsMetadata);
            close();

            insertPreparedStatement = connection.prepareStatement(insertSql);
            insertStatementBinder = new QueryStatementBinder(
                    insertPreparedStatement,
                    topicSchemas,
                    kafkaFieldsMetadata
            );
        }

        records.add(record);

        if (records.size() >= config.batchSize) {
            flushed.addAll(flush());
        }
        return flushed;
    }

    public List<SinkRecord> flush() throws SQLException {
        if (records.isEmpty()) {
            return List.of();
        }
        log.info("Flushing {} buffered records", records.size());
        for (SinkRecord record : records) {
            insertStatementBinder.bind(record);
        }
        insertAndCommit();

        // Switching batches as now flushed
        List<SinkRecord> flushedRecords = records;
        records = new ArrayList<>();
        return flushedRecords;
    }

    private void insertAndCommit() throws SQLException {
        int[] batchStatus = insertPreparedStatement.executeBatch();
        log.info(String.format("Executing %d batches.", batchStatus.length));
        for (int updateCount : batchStatus) {
            if (updateCount == Statement.EXECUTE_FAILED) {
                throw new BatchUpdateException("Execution failed for part of the batch insert", batchStatus);
            }
        }
    }


    public void close() throws SQLException {
        log.debug("Closing BufferedRecords with updatePreparedStatement: {}", insertPreparedStatement);
        if (nonNull(insertPreparedStatement)) {
            insertPreparedStatement.close();
            insertPreparedStatement = null;
        }
    }

}