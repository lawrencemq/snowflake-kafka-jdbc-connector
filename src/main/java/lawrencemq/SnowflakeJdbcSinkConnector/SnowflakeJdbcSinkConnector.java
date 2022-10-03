package lawrencemq.SnowflakeJdbcSinkConnector;

import lawrencemq.SnowflakeJdbcSinkConnector.sink.SnowflakeJdbcSinkConnectorConfig;
import lawrencemq.SnowflakeJdbcSinkConnector.sink.SnowflakeJdbcSinkTask;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static lawrencemq.SnowflakeJdbcSinkConnector.sink.Utils.getVersion;

public class SnowflakeJdbcSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(SnowflakeJdbcSinkConnector.class);

    private Map<String, String> configProps;

    @Override
    public Class<? extends Task> taskClass() {
        return SnowflakeJdbcSinkTask.class;
    }

    @Override
    public void start(Map<String, String> props) {
        configProps = props;
    }


    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Creating {} workers.", maxTasks);
        return IntStream.range(0, maxTasks)
                .mapToObj(i -> configProps)
                .collect(Collectors.toList());
    }

    @Override
    public void stop() {
        // no op
    }

    @Override
    public Config validate(Map<String, String> connectorConfigs) {
        Config config = super.validate(connectorConfigs);

        return config;
    }


    @Override
    public ConfigDef config() {
        return SnowflakeJdbcSinkConnectorConfig.CONFIG_DEF;
    }

    @Override
    public String version() {
        return getVersion();
    }
}
