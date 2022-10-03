package lawrencemq.SnowflakeJdbcSinkConnector.sink;


import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.types.Password;

import java.util.Map;

public class SnowflakeJdbcSinkConnectorConfig extends AbstractConfig {
    private static final String SNOWFLAKE_CONNECTION_GROUP = "Snowflake Connection";
    public static final String SNOWFLAKE_USER_NAME = "snowflake.username";
    public static final String SNOWFLAKE_PASSPHRASE = "snowflake.passphrase";
    public static final String SNOWFLAKE_ACCOUNT = "snowflake.account";
    public static final String SNOWFLAKE_WAREHOUSE = "snowflake.warehouse";
    public static final String SNOWFLAKE_ROLE = "snowflake.role";
    public static final String SNOWFLAKE_DB = "snowflake.database";
    public static final String SNOWFLAKE_SCHEMA = "snowflake.schema";
    public static final String SNOWFLAKE_TABLE = "snowflake.table";
    public static final String SNOWFLAKE_PRIVATE_KEY_FILE = "snowflake.private.key.filename";
    public static final String SNOWFLAKE_PRIVATE_KEY_FILE_PASSPHRASE = "snowflake.private.key.passphrase";
    public static final String SNOWFLAKE_MAX_RETRIES = "snowflake.max.retries";
    public static final String SNOWFLAKE_RETRY_BACKOFF_MS = "snowflake.retry.backoffMs";

    public static final String BATCH_SIZE = "batch.size";
    public static final String AUTO_CREATE = "auto.create";
    public static final String AUTO_EVOLVE = "auto.evolve";
    public static final String IGNORE_KEY = "ignore.kafka.message.key";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(SNOWFLAKE_USER_NAME,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    "Snowflake username to use",
                    SNOWFLAKE_CONNECTION_GROUP,
                    1,
                    ConfigDef.Width.SHORT,
                    "Snowflake username"
            )
            .define(SNOWFLAKE_ACCOUNT,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    "Snowflake account identifier for connections, e.g., https://<account identifier>.snowflakecomputing.com/",
                    SNOWFLAKE_CONNECTION_GROUP,
                    2,
                    ConfigDef.Width.SHORT,
                    "Snowflake account identifier"
            )
            .define(SNOWFLAKE_PASSPHRASE,
                    ConfigDef.Type.PASSWORD,
                    "",
                    ConfigDef.Importance.HIGH,
                    "Snowflake users may connect with a password.",
                    SNOWFLAKE_CONNECTION_GROUP,
                    3,
                    ConfigDef.Width.SHORT,
                    "Snowflake password for username"
                    )
            .define(SNOWFLAKE_WAREHOUSE,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    "Snowflake warehouse to use",
                    SNOWFLAKE_CONNECTION_GROUP,
                    4,
                    ConfigDef.Width.SHORT,
                    "Snowflake warehouse"
            )
            .define(SNOWFLAKE_ROLE,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    "Snowflake user role to use",
                    SNOWFLAKE_CONNECTION_GROUP,
                    5,
                    ConfigDef.Width.SHORT,
                    "Snowflake user role"
            )
            .define(SNOWFLAKE_DB,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    "Destination Snowflake Database",
                    SNOWFLAKE_CONNECTION_GROUP,
                    6,
                    ConfigDef.Width.SHORT,
                    "Database Name"
            )
            .define(SNOWFLAKE_SCHEMA,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    "Destination Snowflake Schema",
                    SNOWFLAKE_CONNECTION_GROUP,
                    7,
                    ConfigDef.Width.SHORT,
                    "Schema Name"
            )
            .define(SNOWFLAKE_TABLE,
                    ConfigDef.Type.STRING,
                    ConfigDef.Importance.HIGH,
                    "Destination Snowflake Table",
                    SNOWFLAKE_CONNECTION_GROUP,
                    8,
                    ConfigDef.Width.SHORT,
                    "Table Name"
            )
            .define(SNOWFLAKE_PRIVATE_KEY_FILE,
                    ConfigDef.Type.STRING,
                    "",
                    ConfigDef.Importance.HIGH,
                    "Snowflake usernames may connect with an RSA private key. Provide the filename and its location.",
                    SNOWFLAKE_CONNECTION_GROUP,
                    9,
                    ConfigDef.Width.SHORT,
                    "Snowflake user private-key file"
            )
            .define(SNOWFLAKE_PRIVATE_KEY_FILE_PASSPHRASE,
                    ConfigDef.Type.PASSWORD,
                    "",
                    ConfigDef.Importance.MEDIUM,
                    "RSA Private keys may be encrypted locally. Enter the decryption passphrase here.",
                    SNOWFLAKE_CONNECTION_GROUP,
                    10,
                    ConfigDef.Width.SHORT,
                    "Snowflake private key passphrase"
            )
            .define(SNOWFLAKE_MAX_RETRIES,
                    ConfigDef.Type.INT,
                    3,
                    ConfigDef.Importance.LOW,
                    "Maximum number of times the connector will attempt to connect to Snowflake before failing.",
                    SNOWFLAKE_CONNECTION_GROUP,
                    11,
                    ConfigDef.Width.SHORT,
                    "Snowflake maximum reconnect retries"
            )
            .define(SNOWFLAKE_RETRY_BACKOFF_MS,
                    ConfigDef.Type.LONG,
                    5_000,
                    ConfigDef.Importance.LOW,
                    "The time in milliseconds to wait following an error before a retry attempt is made.",
                    SNOWFLAKE_CONNECTION_GROUP,
                    12,
                    ConfigDef.Width.SHORT,
                    "JDBC Backoff milliseconds"
            )
            .define(
                    BATCH_SIZE,
                    ConfigDef.Type.INT,
                    3_000,
                    ConfigDef.Importance.LOW,
                    "Specifies how many records to attempt to batch together for insertion into the destination table, when possible.",
                    SNOWFLAKE_CONNECTION_GROUP,
                    13,
                    ConfigDef.Width.SHORT,
                    "Batch Size"
            )
            .define(
                    AUTO_CREATE,
                    ConfigDef.Type.BOOLEAN,
                    true,
                    ConfigDef.Importance.LOW,
                    "Whether to automatically create the destination table based on record schema if it is found to be missing by issuing ``CREATE``.",
                    SNOWFLAKE_CONNECTION_GROUP,
                    14,
                    ConfigDef.Width.SHORT,
                    "Auto-Create"
            )
            .define(
                    AUTO_EVOLVE,
                    ConfigDef.Type.BOOLEAN,
                    true,
                    ConfigDef.Importance.LOW,
                    "Whether to automatically add columns in the table schema when found to be missing relative to the record schema by issuing ``ALTER``.",
                    SNOWFLAKE_CONNECTION_GROUP,
                    15,
                    ConfigDef.Width.SHORT,
                    "Auto-Evolve"
            )
            .define(
                    IGNORE_KEY,
                    ConfigDef.Type.BOOLEAN,
                    false,
                    ConfigDef.Importance.LOW,
                    "Whether ignore/skip key values from messages.",
                    SNOWFLAKE_CONNECTION_GROUP,
                    16,
                    ConfigDef.Width.SHORT,
                    "Ignore Kafka Message Keys"
            );


    public final String username;
    public final String accountIdentifier;
    public final String warehouse;
    public final String role;
    public final String db;
    public final String schema;
    public final String table;
    public final String privateKeyFile;
    public final Password privateKeyFilePassphrase;
    public final Password passphrase;
    public final int maxRetries;
    public final long retryBackoffMs;

    public final int batchSize;
    public final boolean autoCreate;
    public final boolean autoEvolve;
    public final boolean ignoreKey;


    public SnowflakeJdbcSinkConnectorConfig(Map<?, ?> props) {
        super(configDef(), props);

        this.accountIdentifier = this.getString(SNOWFLAKE_ACCOUNT);
        this.username = this.getString(SNOWFLAKE_USER_NAME);
        this.warehouse = this.getString(SNOWFLAKE_WAREHOUSE);
        this.role = this.getString(SNOWFLAKE_ROLE);
        this.db = this.getString(SNOWFLAKE_DB).toUpperCase();
        this.schema = this.getString(SNOWFLAKE_SCHEMA).toUpperCase();
        this.table = this.getString(SNOWFLAKE_TABLE).toUpperCase();
        this.privateKeyFile = this.getString(SNOWFLAKE_PRIVATE_KEY_FILE);
        this.privateKeyFilePassphrase = this.getPassword(SNOWFLAKE_PRIVATE_KEY_FILE_PASSPHRASE);
        this.passphrase = this.getPassword(SNOWFLAKE_PASSPHRASE);
        this.maxRetries = this.getInt(SNOWFLAKE_MAX_RETRIES);
        this.retryBackoffMs = this.getLong(SNOWFLAKE_RETRY_BACKOFF_MS);
        this.batchSize = this.getInt(BATCH_SIZE);
        this.autoCreate = this.getBoolean(AUTO_CREATE);
        this.autoEvolve = this.getBoolean(AUTO_EVOLVE);
        this.ignoreKey = this.getBoolean(IGNORE_KEY);
    }

    protected static ConfigDef configDef() {
        return CONFIG_DEF;
    }
}
