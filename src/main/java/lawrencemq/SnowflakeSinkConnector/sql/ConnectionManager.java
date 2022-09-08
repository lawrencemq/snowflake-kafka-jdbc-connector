package lawrencemq.SnowflakeSinkConnector.sql;


import lawrencemq.SnowflakeSinkConnector.sink.SnowflakeSinkConnectorConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;

import static lawrencemq.SnowflakeSinkConnector.sql.SnowflakeConnection.getSnowflakeConnection;


public final class ConnectionManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);

    private final SnowflakeSinkConnectorConfig config;
    private final int maxConnectionAttempts;
    private final long connectionRetryBackoff;

    private Connection connection;

    public ConnectionManager(SnowflakeSinkConnectorConfig config) {
        this.config = config;
        this.maxConnectionAttempts = config.maxRetries;
        this.connectionRetryBackoff = config.retryBackoffMs;
    }

    public synchronized Connection getConnection() {
        try {
            if (!isConnectionValid( 10)) {
                log.info("The database connection is invalid. Making new connection...");
                reconnect();
            }
        } catch (SQLException e) {
            throw new ConnectException(e);
        }
        return connection;
    }
    private void reconnect() throws SQLException {
        close();
        newConnection();
    }

    private boolean isConnectionValid(int timeout) {
        if(Objects.isNull(connection)){
            return false;
        }
        try {
            return connection.isValid(timeout);
        } catch (SQLException e) {
            log.debug("Unable to check if the connection to Snowflake is valid", e);
            return false;
        }
    }

    private void newConnection() throws SQLException {
        for (int attempts = 1; attempts <= maxConnectionAttempts; attempts++) {
            log.info("Attempt {} to open connection to {}", attempts, this);
            try {
                connection = getSnowflakeConnection(config);
                connection.setAutoCommit(false);
                log.info("Snowflake JDBC writer connected.");
                return;
            } catch (SQLException e) {
                if (attempts + 1 > maxConnectionAttempts) {
                    throw e;
                }
                log.warn("Unable to connect to Snowflake. Will retry in {} ms.", connectionRetryBackoff, e);
                try {
                    Thread.sleep(connectionRetryBackoff);
                } catch (InterruptedException ie) {
                    // no-op
                }
            }
        }
    }

    @Override
    public synchronized void close() {
        if (Objects.nonNull(connection)) {
            try {
                log.info("Closing connection to Snowflake");
                connection.close();
            } catch (SQLException e) {
                log.warn("Error while closing connection", e);
            } finally {
                connection = null;
            }
        }
    }


}
