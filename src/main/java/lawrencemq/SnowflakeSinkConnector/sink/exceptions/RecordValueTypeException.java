package lawrencemq.SnowflakeSinkConnector.sink.exceptions;

import org.apache.kafka.connect.errors.*;

public class RecordValueTypeException extends ConnectException {
    public RecordValueTypeException(String message) {
        super(message);
    }
    public RecordValueTypeException(String reason, Throwable throwable) {
        super(reason, throwable);
    }
}
