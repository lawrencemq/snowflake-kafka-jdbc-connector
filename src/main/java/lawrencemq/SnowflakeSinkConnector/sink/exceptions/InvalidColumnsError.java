package lawrencemq.SnowflakeSinkConnector.sink.exceptions;

import org.apache.kafka.connect.errors.*;

public class InvalidColumnsError extends ConnectException {
    public InvalidColumnsError(String message) {
        super(message);
    }
    public InvalidColumnsError(String reason, Throwable throwable) {
        super(reason, throwable);
    }
}
