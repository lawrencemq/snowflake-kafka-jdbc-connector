package lawrencemq.SnowflakeSinkConnector.sql;


import lawrencemq.SnowflakeSinkConnector.sink.SnowflakeSinkConnectorConfig;
import org.apache.kafka.connect.errors.ConnectException;
import org.bouncycastle.asn1.pkcs.*;
import org.bouncycastle.jce.provider.*;
import org.bouncycastle.openssl.*;
import org.bouncycastle.openssl.jcajce.*;
import org.bouncycastle.operator.*;
import org.bouncycastle.pkcs.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.PrivateKey;
import java.security.Security;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;


public class ConnectionManager implements AutoCloseable {

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
        String url = createConnectionUrl();
        Properties props = createConnectionProps();

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

    private Properties createConnectionProps() {

        Properties props = new Properties();
        props.put("user", config.username);
//        try {
//            prop.put("privateKey", PrivateKeyReader.get(config.privateKey, config.passphrase.value()));
//        } catch (IOException e) {
//            throw new RuntimeException("Unable to read private key file.", e);
//        } catch (PKCSException e) {
//            throw new RuntimeException("Unable to decrypt private key file", e);
//        } catch (OperatorCreationException e) {
//            throw new RuntimeException("Unknown error reading private key", e);
//        }

        props.put("private_key_file", config.privateKeyFile);
        props.put("private_key_file_pwd", config.passphrase.value());
        props.put("db", config.db);
        props.put("schema", config.schema);
        props.put("warehouse", config.warehouse);
        props.put("role", config.role);
        return props;
    }

    private String createConnectionUrl() {
        //  "jdbc:snowflake://<account_identifier>.snowflakecomputing.com";
        String url = "jdbc:snowflake://" + config.accountIdentifier + ".snowflakecomputing.com";
        return url;
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
    /**
     * https://docs.snowflake.com/en/user-guide/jdbc-configure.html
     * Under the Sample Code section
     */
    private class PrivateKeyReader {


        public PrivateKey get(String filename, String passphrase) throws IOException, PKCSException, OperatorCreationException {
            PrivateKeyInfo privateKeyInfo = null;
            Security.addProvider(new BouncyCastleProvider());
            // Read an object from the private key file.
            PEMParser pemParser = new PEMParser(new FileReader(Paths.get(filename).toFile()));
            Object pemObject = pemParser.readObject();
            if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo) {
                // Handle the case where the private key is encrypted.
                PKCS8EncryptedPrivateKeyInfo encryptedPrivateKeyInfo = (PKCS8EncryptedPrivateKeyInfo) pemObject;
                InputDecryptorProvider pkcs8Prov = new JceOpenSSLPKCS8DecryptorProviderBuilder().build(passphrase.toCharArray());
                privateKeyInfo = encryptedPrivateKeyInfo.decryptPrivateKeyInfo(pkcs8Prov);
            } else if (pemObject instanceof PrivateKeyInfo) {
                // Handle the case where the private key is unencrypted.
                privateKeyInfo = (PrivateKeyInfo) pemObject;
            }
            pemParser.close();
            JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider(BouncyCastleProvider.PROVIDER_NAME);
            return converter.getPrivateKey(privateKeyInfo);
        }
    }


}
