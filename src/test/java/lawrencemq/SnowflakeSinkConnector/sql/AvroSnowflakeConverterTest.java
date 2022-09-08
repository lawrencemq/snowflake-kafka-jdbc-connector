package lawrencemq.SnowflakeSinkConnector.sql;

import org.apache.kafka.connect.data.*;
import org.junit.jupiter.api.Test;

import java.util.List;

import static lawrencemq.SnowflakeSinkConnector.sql.AvroSnowflakeConverter.getSqlTypeFromAvro;
import static org.apache.kafka.connect.data.Schema.*;
import static org.apache.kafka.connect.data.Schema.Type.*;
import static org.junit.jupiter.api.Assertions.*;

public class AvroSnowflakeConverterTest {

    @Test
    void testGetSqlTypeFromAvroForBasicTypes() {
        List<Schema> integers = List.of(
                INT8_SCHEMA,
                INT16_SCHEMA,
                INT32_SCHEMA,
                INT64_SCHEMA,
                OPTIONAL_INT8_SCHEMA,
                OPTIONAL_INT16_SCHEMA,
                OPTIONAL_INT32_SCHEMA,
                OPTIONAL_INT64_SCHEMA
        );
        List<Schema> floats = List.of(
                FLOAT32_SCHEMA,
                FLOAT64_SCHEMA,
                OPTIONAL_FLOAT32_SCHEMA,
                OPTIONAL_FLOAT64_SCHEMA
        );
        List<Schema> booleans = List.of(
                BOOLEAN_SCHEMA,
                OPTIONAL_BOOLEAN_SCHEMA
        );
        List<Schema> strings = List.of(
                STRING_SCHEMA,
                OPTIONAL_STRING_SCHEMA
        );
        List<Schema> bytes = List.of(
                BYTES_SCHEMA,
                OPTIONAL_BYTES_SCHEMA
        );


        Schema arraySchema = SchemaBuilder.array(STRING_SCHEMA).build();
        Schema mapSchema = SchemaBuilder.map(STRING_SCHEMA, STRING_SCHEMA).build();
        Schema structSchema = SchemaBuilder.struct().build();

        for(Schema intType: integers){
            assertEquals(getSqlTypeFromAvro(intType), "NUMBER");
        }

        for(Schema floatType: floats){
            assertEquals(getSqlTypeFromAvro(floatType), "FLOAT");
        }

        for(Schema booleanType: booleans){
            assertEquals(getSqlTypeFromAvro(booleanType), "BOOLEAN");
        }

        for(Schema stringType: strings){
            assertEquals(getSqlTypeFromAvro(stringType), "TEXT");
        }

        for(Schema byteType: bytes){
            assertEquals(getSqlTypeFromAvro(byteType), "BINARY");
        }

        assertEquals(getSqlTypeFromAvro(arraySchema), "ARRAY");
        assertEquals(getSqlTypeFromAvro(mapSchema), "VARIANT");
        assertEquals(getSqlTypeFromAvro(structSchema), "VARIANT");

    }

    @Test
    void testGetSqlTypeFromAvroForComplexTypes() {
        List<Schema> decimals = List.of(
                Decimal.builder(2).build(),
                Decimal.builder(2).optional().build()
        );
        List<Schema> dates = List.of(
                Date.builder().build(),
                Date.builder().optional().build()
        );
        List<Schema> timestamps = List.of(
                Time.builder().build(),
                Time.builder().optional().build()
        );
        List<Schema> kafkaTimestamps = List.of(
                Timestamp.builder().build(),
                Timestamp.builder().optional().build()
        );

        for(Schema decimalType: decimals){
            assertEquals(getSqlTypeFromAvro(decimalType), "NUMBER");
        }

        for(Schema dateType: dates){
            assertEquals(getSqlTypeFromAvro(dateType), "DATE");
        }

        for(Schema timeType: timestamps){
            assertEquals(getSqlTypeFromAvro(timeType), "TIMESTAMP_TZ");
        }

        for(Schema timeType: kafkaTimestamps){
            assertEquals(getSqlTypeFromAvro(timeType), "TIMESTAMP_TZ");
        }
    }

    @Test
    void formatColumnValue() {
    }

    @Test
    void bind() {
    }
}