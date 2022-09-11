package lawrencemq.SnowflakeSinkConnector.sink;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KafkaColumnMetadataTest {
    KafkaColumnMetadata kcm1 = new KafkaColumnMetadata("testColumn1", SchemaBuilder.string().optional().build());
    KafkaColumnMetadata kcm2 = new KafkaColumnMetadata("testColumn2", SchemaBuilder.float64().optional().build());

    @Test
    void testEquals() {
        assertNotEquals(kcm1, kcm2);
        assertEquals(kcm1, kcm1);
    }

    @Test
    void testToString() {
        assertEquals(kcm1.toString(), "KafkaColumnMetadata{columnName=testColumn1,schema=Schema{STRING}}");
    }
}