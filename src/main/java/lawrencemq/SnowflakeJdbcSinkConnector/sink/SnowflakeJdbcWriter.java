package lawrencemq.SnowflakeJdbcSinkConnector.sink;

import lawrencemq.SnowflakeJdbcSinkConnector.sink.exceptions.TableAlterOrCreateException;
import lawrencemq.SnowflakeJdbcSinkConnector.sql.ConnectionManager;
import org.apache.kafka.connect.sink.SinkRecord;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collection;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeJdbcWriter {
    private static final Logger log = LoggerFactory.getLogger(SnowflakeJdbcWriter.class);

    private final SnowflakeJdbcSinkConnectorConfig config;
    private final TableManager tableManager;
    private final ConnectionManager connectionManager;

    SnowflakeJdbcWriter(SnowflakeJdbcSinkConnectorConfig config, TableManager tableManager, ConnectionManager connectionManager) {
        this.config = config;
        this.tableManager = tableManager;
        this.connectionManager = connectionManager;
    }


    void write(Collection<SinkRecord> records) throws SQLException, TableAlterOrCreateException {
        if(records.size() == 0){
            return;
        }

        log.info("Received {} records to push to Snowflake", records.size());
        Connection connection = connectionManager.getConnection();
        RecordBuffer bufferForTable = new RecordBuffer(config, tableManager, connection);
        bufferForTable.addAll(records);

        log.info("Flushing Snowflake JDBC Writer for table: {}", tableManager.getTable());

        try {
            bufferForTable.flush();
            bufferForTable.close();
            connection.commit();
        } catch (SQLException | TableAlterOrCreateException e) {
            try {
                connection.rollback();
            } catch (SQLException e2) {
                e.addSuppressed(e2);
            } finally {
                throw e;
            }
        }
    }

    void close() {
        connectionManager.close();
    }

}