package lawrencemq.SnowflakeSinkConnector.sql;

import lawrencemq.SnowflakeSinkConnector.sink.SnowflakeSinkConnectorConfig;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.security.PrivateKey;
import java.security.Security;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

final class SnowflakeConnection {

    static Connection getSnowflakeConnection(SnowflakeSinkConnectorConfig config) throws SQLException {
        //  "jdbc:snowflake://<account_identifier>.snowflakecomputing.com";
        String url = "jdbc:snowflake://" + config.accountIdentifier + ".snowflakecomputing.com";
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

        return DriverManager.getConnection(url, props);
    }
    /**
     * https://docs.snowflake.com/en/user-guide/jdbc-configure.html
     * Under the Sample Code section
     */
    private static class PrivateKeyReader {


        public static PrivateKey get(String filename, String passphrase) throws IOException, PKCSException, OperatorCreationException {
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
