package lawrencemq.SnowflakeJdbcSinkConnector.sink;

import org.apache.kafka.connect.data.*;
import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class TopicSchemasTest {

    @Test
    void testEquals() {
        TopicSchemas schemaSet1 = new TopicSchemas(SchemaBuilder.STRING_SCHEMA, Schema.STRING_SCHEMA);
        TopicSchemas schemaSet2 = new TopicSchemas(SchemaBuilder.STRING_SCHEMA, Schema.INT32_SCHEMA);
        TopicSchemas schemaSet3 = new TopicSchemas(SchemaBuilder.STRING_SCHEMA, Schema.STRING_SCHEMA);

        assertNotEquals(schemaSet1, schemaSet2);
        assertEquals(schemaSet1, schemaSet3);
    }

    @Test
    void testHashCode(){
        TopicSchemas schemaSet1 = new TopicSchemas(SchemaBuilder.STRING_SCHEMA, Schema.STRING_SCHEMA);
        TopicSchemas schemaSet2 = new TopicSchemas(SchemaBuilder.STRING_SCHEMA, Schema.INT32_SCHEMA);
        TopicSchemas schemaSet3 = new TopicSchemas(SchemaBuilder.STRING_SCHEMA, Schema.STRING_SCHEMA);

        Set<TopicSchemas> schemasSet = new HashSet<>();
        schemasSet.add(schemaSet1);
        schemasSet.add(schemaSet2);
        schemasSet.add(schemaSet3);
        assertEquals(2, schemasSet.size());
    }

    @Test
    void testToString() {
        TopicSchemas schemaSet = new TopicSchemas(
                SchemaBuilder.string()
                        .name("id")
                        .defaultValue("42")
                        .build(),
                SchemaBuilder.struct()
                        .name("data")
                        .field("lat", SchemaBuilder.float64().name("lat").build())
                        .field("long", SchemaBuilder.float64().name("long").build())
                        .build()
        );
        String expectedStr = "TopicSchemas{keySchema=Schema{id:STRING},valueSchema=Schema{data:STRUCT}}";
        assertEquals(expectedStr, schemaSet.toString());
    }
}