package lawrencemq.SnowflakeJdbcSinkConnector.sink;

import org.apache.kafka.connect.errors.*;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

import static lawrencemq.SnowflakeJdbcSinkConnector.TestData.DEFAULT_CONFIGS;
import static lawrencemq.SnowflakeJdbcSinkConnector.sink.RecordBufferTest.generateMessages;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class SnowflakeJdbcSinkTaskTest {

    SnowflakeJdbcSinkTask task;

    @BeforeEach
    void setupSinkTask(){
        task = new TestSnowflakeJdbcSinkTask();
        task.start(DEFAULT_CONFIGS);
    }

    @Test
    void doesNothingWhenGivenNoRecords() throws SQLException {
        task.put(List.of());
        verify(task.writer, never()).write(any());
    }

    @Test
    void put() throws SQLException {

        List<SinkRecord> sinkRecords = generateMessages(10);

        task.put(sinkRecords);
        verify(task.writer, times(1)).write(eq(sinkRecords));

    }

    @Test
    void putHandlesRetries() throws SQLException {

        doThrow(new SQLException("Test Exception")).when(task.writer).write(anyCollection());

        List<SinkRecord> sinkRecords = generateMessages(10);

        assertEquals(task.remainingRetries, 3);
        assertThrows(RetriableException.class,
                () -> task.put(sinkRecords) ,
                "Should throw a RetriableException so Sink task is retried.");
        assertEquals(task.remainingRetries, 2);

    }

    @Test
    void putThrowsIfRetriesExhausted() throws SQLException {
        List<SinkRecord> sinkRecords = generateMessages(10);

        int startingRemainingRetries = 3;

        assertEquals(task.remainingRetries, startingRemainingRetries);
        for(int i = startingRemainingRetries; i > 0; i--) {
            doThrow(new SQLException("Test Exception")).when(task.writer).write(anyCollection());

            assertThrows(RetriableException.class,
                    () -> task.put(sinkRecords),
                    "Should throw a RetriableException so Sink task is retried.");
        }

        doThrow(new SQLException("Test Exception")).when(task.writer).write(anyCollection());
        assertThrows(ConnectException.class,
                () -> task.put(sinkRecords),
                "Should throw failing exception if retries are exhausted.");

    }

    @Test
    void stop() {
        task.stop();
        verify(task.writer).close();
    }

    private static class TestSnowflakeJdbcSinkTask extends SnowflakeJdbcSinkTask {

        @Override
        protected void createWriter(){
            this.writer = mock(SnowflakeJdbcWriter.class);
        }
    }
}