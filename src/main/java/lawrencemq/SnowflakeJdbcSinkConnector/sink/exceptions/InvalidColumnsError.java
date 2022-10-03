package lawrencemq.SnowflakeJdbcSinkConnector.sink.exceptions;

import org.apache.kafka.connect.errors.ConnectException;

public class InvalidColumnsError extends ConnectException {
    public InvalidColumnsError(String message) {
        super(message);
    }
}
