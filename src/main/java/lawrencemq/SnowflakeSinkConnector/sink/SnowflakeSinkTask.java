package lawrencemq.SnowflakeSinkConnector.sink;

import lawrencemq.SnowflakeSinkConnector.sql.Table;
import lawrencemq.SnowflakeSinkConnector.sql.ConnectionManager;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.*;

import static lawrencemq.SnowflakeSinkConnector.sink.Utils.getVersion;

public class SnowflakeSinkTask extends SinkTask {

    private static final Logger log = LoggerFactory.getLogger(SnowflakeSinkTask.class);

    SnowflakeSinkConnectorConfig config;
    SnowflakeJdbcWriter writer;
    int remainingRetries;

    @Override
    public String version() {
        return getVersion();
    }

    @Override
    public void start(Map<String, String> props) {
        log.info("Starting Snowflake JDBC Sink task");
        this.config = new SnowflakeSinkConnectorConfig(props);
        this.createWriter();

        this.remainingRetries = this.config.maxRetries;

    }

    protected void createWriter() {
        ConnectionManager connectionManager = new ConnectionManager(config);

        Table destinationTable = new Table(config.db, config.schema, config.table);

        TableManager tableManager = new TableManager(config, destinationTable);

        this.writer = new SnowflakeJdbcWriter(
                config,
                tableManager,
                connectionManager
        );
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        log.debug("Received {} records.", records.size());

        if (!records.isEmpty()) {
            try {
                writer.write(records);
                remainingRetries = config.maxRetries;
            } catch (SQLException e) {
                log.warn("Write of {} records failed, remainingRetries: {}", records.size(), remainingRetries);
                SQLException sqlAllMessagesException = getAllMessagesException(e);
                if (remainingRetries == 0) {
                    log.error("Failing task. Max retries reached.");
                    throw new ConnectException(sqlAllMessagesException);
                }

                recreateWriter();
                remainingRetries--;
                if(Objects.nonNull(context)) {
                    context.timeout(config.retryBackoffMs);
                }
                throw new RetriableException(sqlAllMessagesException);
            }
        }
    }

    private void recreateWriter() {
        writer.close();
        createWriter();
    }

    private SQLException getAllMessagesException(SQLException e) {
        StringBuilder allSqlErrors = new StringBuilder("Encountered exceptions:" + System.lineSeparator());
        for (Throwable t : e) {
            allSqlErrors.append(t).append(System.lineSeparator());
        }
        SQLException sqlAllMessagesException = new SQLException(allSqlErrors.toString());
        sqlAllMessagesException.setNextException(e);
        return sqlAllMessagesException;
    }

    @Override
    public void stop() {
        log.info("Stopping task");
        writer.close();
    }
}
