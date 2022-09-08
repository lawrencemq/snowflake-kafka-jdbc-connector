package lawrencemq.SnowflakeSinkConnector.sql;

import org.apache.kafka.connect.data.*;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import static lawrencemq.SnowflakeSinkConnector.sql.AvroSnowflakeConverter.formatColumnValue;
import static lawrencemq.SnowflakeSinkConnector.sql.AvroSnowflakeConverter.getSqlTypeFromAvro;
import static org.apache.kafka.connect.data.Schema.*;
import static org.junit.jupiter.api.Assertions.*;

public class AvroSnowflakeConverterTest {
    private static final List<Schema> INTEGERS = List.of(
            INT8_SCHEMA,
            INT16_SCHEMA,
            INT32_SCHEMA,
            INT64_SCHEMA,
            OPTIONAL_INT8_SCHEMA,
            OPTIONAL_INT16_SCHEMA,
            OPTIONAL_INT32_SCHEMA,
            OPTIONAL_INT64_SCHEMA
    );
    private static final List<Schema> FLOATS = List.of(
            FLOAT32_SCHEMA,
            FLOAT64_SCHEMA,
            OPTIONAL_FLOAT32_SCHEMA,
            OPTIONAL_FLOAT64_SCHEMA
    );
    private static final List<Schema> BOOLEANS = List.of(
            BOOLEAN_SCHEMA,
            OPTIONAL_BOOLEAN_SCHEMA
    );
    private static final List<Schema> STRINGS = List.of(
            STRING_SCHEMA,
            OPTIONAL_STRING_SCHEMA
    );
    private static final List<Schema> BYTES = List.of(
            BYTES_SCHEMA,
            OPTIONAL_BYTES_SCHEMA
    );

    private static final Schema ARRAY_SCHEMA = SchemaBuilder.array(STRING_SCHEMA).build();
    private static final Schema MAP_SCHEMA = SchemaBuilder.map(STRING_SCHEMA, STRING_SCHEMA).build();
    private static final Schema STRUCT_SCHEMA = SchemaBuilder.struct().build();
    private static final List<Schema> DECIMALS = List.of(
            Decimal.builder(2).build(),
            Decimal.builder(2).optional().build()
    );
    private static final List<Schema> DATES = List.of(
            Date.builder().build(),
            Date.builder().optional().build()
    );
    private static final List<Schema> TIMESTAMPS = List.of(
            Time.builder().build(),
            Time.builder().optional().build()
    );
    private static final List<Schema> KAFKA_TIMESTAMPS = List.of(
            Timestamp.builder().build(),
            Timestamp.builder().optional().build()
    );
    @Test
    void testGetSqlTypeFromAvroForBasicTypes() {

        for(Schema intType: INTEGERS){
            assertEquals(getSqlTypeFromAvro(intType), "NUMBER");
        }

        for(Schema floatType: FLOATS){
            assertEquals(getSqlTypeFromAvro(floatType), "FLOAT");
        }

        for(Schema booleanType: BOOLEANS){
            assertEquals(getSqlTypeFromAvro(booleanType), "BOOLEAN");
        }

        for(Schema stringType: STRINGS){
            assertEquals(getSqlTypeFromAvro(stringType), "TEXT");
        }

        for(Schema byteType: BYTES){
            assertEquals(getSqlTypeFromAvro(byteType), "BINARY");
        }

        assertEquals(getSqlTypeFromAvro(ARRAY_SCHEMA), "ARRAY");
        assertEquals(getSqlTypeFromAvro(MAP_SCHEMA), "VARIANT");
        assertEquals(getSqlTypeFromAvro(STRUCT_SCHEMA), "VARIANT");

    }

    @Test
    void testGetSqlTypeFromAvroForComplexTypes() {

        for(Schema decimalType: DECIMALS){
            assertEquals(getSqlTypeFromAvro(decimalType), "NUMBER");
        }

        for(Schema dateType: DATES){
            assertEquals(getSqlTypeFromAvro(dateType), "DATE");
        }

        for(Schema timeType: TIMESTAMPS){
            assertEquals(getSqlTypeFromAvro(timeType), "TIMESTAMP_TZ");
        }

        for(Schema timeType: KAFKA_TIMESTAMPS){
            assertEquals(getSqlTypeFromAvro(timeType), "TIMESTAMP_TZ");
        }
    }

    @Test
    void testFormatColumnValueForBasicTypes() {

        for(Schema intSchema : INTEGERS) {
            QueryBuilder queryBuilder = QueryBuilder.newQuery();
            formatColumnValue(queryBuilder, intSchema, 42);
            assertEquals(queryBuilder.toString(), "42");
        }

        for(Schema floatSchema : FLOATS) {
            QueryBuilder queryBuilder = QueryBuilder.newQuery();
            formatColumnValue(queryBuilder, floatSchema, 42.069);
            assertEquals(queryBuilder.toString(), "42.069");
        }

        for(Schema booleanSchema : BOOLEANS) {
            QueryBuilder queryBuilder = QueryBuilder.newQuery();
            formatColumnValue(queryBuilder, booleanSchema, true);
            formatColumnValue(queryBuilder, booleanSchema, false);
            assertEquals(queryBuilder.toString(), "10");
        }

        for(Schema stringSchema : STRINGS) {
            QueryBuilder queryBuilder = QueryBuilder.newQuery();
            formatColumnValue(queryBuilder, stringSchema, "TheGooseIsLoose");
            assertEquals(queryBuilder.toString(), "'TheGooseIsLoose'");
        }

        for(Schema bytesSchema : BYTES) {
            QueryBuilder queryBuilder = QueryBuilder.newQuery();
            formatColumnValue(queryBuilder, bytesSchema, "TheGooseIsLoose".getBytes());
            assertEquals(queryBuilder.toString(), "x'546865476F6F736549734C6F6F7365'");
        }

        for(Schema bytesSchema : BYTES) {
            QueryBuilder queryBuilder = QueryBuilder.newQuery();
            ByteBuffer buffer = ByteBuffer.wrap("TheGooseIsLoose".getBytes());
            formatColumnValue(queryBuilder, bytesSchema, buffer);
            assertEquals(queryBuilder.toString(), "x'546865476F6F736549734C6F6F7365'");
        }
    }

    @Test
    void testFormatColumnValueForDataStructs() {
        Schema arraySchema = SchemaBuilder.array(STRING_SCHEMA).build();
        Schema mapSchema = SchemaBuilder.map(STRING_SCHEMA, STRING_SCHEMA).build();
        Schema structSchema = SchemaBuilder.struct().build();

        QueryBuilder arrQueryBuild = QueryBuilder.newQuery();
        formatColumnValue(arrQueryBuild, arraySchema, List.of(1, 2, 3));
        assertEquals(arrQueryBuild.toString(), "[1,2,3]");

        QueryBuilder mapQueryBuild = QueryBuilder.newQuery();
        formatColumnValue(mapQueryBuild, mapSchema, Map.of(1, 2));
        assertEquals(mapQueryBuild.toString(), "{\"1\":2}");

        QueryBuilder structQueryBuild = QueryBuilder.newQuery();
        formatColumnValue(structQueryBuild, structSchema, Map.of(3, 4));
        assertEquals(structQueryBuild.toString(), "{\"3\":4}");

    }
    @Test
    void testFormatColumnValueForComplexTypes() {

        for(Schema decimalType: DECIMALS){
            QueryBuilder queryBuilder = QueryBuilder.newQuery();
            formatColumnValue(queryBuilder, decimalType, "42.690");
            assertEquals(queryBuilder.toString(), "42.690");
        }

        for(Schema dateType: DATES){
            QueryBuilder queryBuilder = QueryBuilder.newQuery();
            formatColumnValue(queryBuilder, dateType, new java.util.Date(1662668958000L));
            assertEquals(queryBuilder.toString(), "'2022-09-08'");
        }

        for(Schema timeType: TIMESTAMPS){
            QueryBuilder queryBuilder = QueryBuilder.newQuery();
            formatColumnValue(queryBuilder, timeType, new java.util.Date(1662668958000L));
            assertEquals(queryBuilder.toString(), "'20:29:18.000'");
        }

        for(Schema timeType: KAFKA_TIMESTAMPS){
            QueryBuilder queryBuilder = QueryBuilder.newQuery();
            formatColumnValue(queryBuilder, timeType, new java.util.Date(1662668958000L));
            assertEquals(queryBuilder.toString(), "'2022-09-08 20:29:18.000'");
        }
    }

    @Test
    void testBindForBasicTypes() {
    }

    @Test
    void testBindForComplexTypes() {
    }
}