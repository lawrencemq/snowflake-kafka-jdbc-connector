package lawrencemq.SnowflakeJdbcSinkConnector.sql;


import lawrencemq.SnowflakeJdbcSinkConnector.sink.SnowflakeJdbcSinkConnectorConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;


public class ConnectionManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(ConnectionManager.class);

    private final SnowflakeJdbcSinkConnectorConfig config;
    private final int maxConnectionAttempts;
    private final long connectionRetryBackoff;

    private Connection connection;

    public ConnectionManager(SnowflakeJdbcSinkConnectorConfig config) {
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
        String url = createConnectionUrl();
        Properties props = createConnectionProps();

        log.info("JDBC connection URL: " + url);
        log.info("JDBC connection properties: " + props.entrySet()
                .stream()
                .map(e -> e.getKey() + ":" + e.getValue())
                .collect(Collectors.joining(", ")));

        for (int attemptCount = 1; attemptCount <= maxConnectionAttempts; attemptCount++) {
            log.info("Attempt {} to open connection to {}", attemptCount, url);
            try {
                connection = getSnowflakeConnection(url, props);
                connection.setAutoCommit(false);
                log.info("New Snowflake JDBC connected successfully.");
                return;
            } catch (SQLException e) {
                if (attemptCount + 1 > maxConnectionAttempts) {
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

    /**
     * https://docs.snowflake.com/en/user-guide/jdbc-configure.html
     * https://docs.snowflake.com/en/user-guide/jdbc-configure.html#private-key-file-name-and-password-as-connection-properties
     * @return
     */
    private Properties createConnectionProps() {

        Properties props = new Properties();
        props.put("user", config.username);
        props.put("db", config.db);
        props.put("schema", config.schema);
        props.put("warehouse", config.warehouse);
        props.put("role", config.role);
        if(config.privateKeyFile.length() > 0){
            props.put("private_key_file", config.privateKeyFile);
        }
        if(config.privateKeyFilePassphrase.value().length() > 0){
            props.put("private_key_file_pwd", config.privateKeyFilePassphrase.value());
        }
        if(config.passphrase.value().length() > 0) {
            props.put("password", "2i5DDQLGiZP7XbX");
        }


        return props;
    }

    private String createConnectionUrl() {
        //  "jdbc:snowflake://<account_identifier>.snowflakecomputing.com";
        return "jdbc:snowflake://" + config.accountIdentifier + ".snowflakecomputing.com";
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


    protected Connection getSnowflakeConnection(String url, Properties props) throws SQLException {
        return DriverManager.getConnection(url, props);
    }

}
