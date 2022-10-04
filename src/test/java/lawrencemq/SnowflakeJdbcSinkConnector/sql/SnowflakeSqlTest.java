package lawrencemq.SnowflakeJdbcSinkConnector.sql;

import org.apache.kafka.connect.data.*;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class SnowflakeSqlTest {

    private static final Table TABLE = new Table("db1", "schema1", "table1");


    @Test
    void buildInsertStatement() {
        LinkedHashMap<String, Field> fieldsToSchemaMap = new LinkedHashMap<>();
        fieldsToSchemaMap.put("robot_id", new Field("robot_id", 0, SchemaBuilder.INT16_SCHEMA));
        fieldsToSchemaMap.put("robot_loc", new Field("robot_loc", 1, SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build()));

        String result = SnowflakeSql.buildInsertStatement(TABLE, fieldsToSchemaMap);
        assertEquals(result, "INSERT INTO \"DB1\".\"SCHEMA1\".\"TABLE1\"(\nROBOT_ID,\nROBOT_LOC) VALUES (?,parse_json(?))\n");

    }

    @Test
    void buildCreateTableStatement() {

        Collection<Field> fields = List.of(
                new Field("helloWorld", 0, SchemaBuilder.string().optional().build()),
                new Field("id", 1, SchemaBuilder.int32().defaultValue(22).build())
        );
        String result = SnowflakeSql.buildCreateTableStatement(TABLE, fields);
        assertEquals(result, "CREATE TABLE \"DB1\".\"SCHEMA1\".\"TABLE1\" (\nHELLOWORLD TEXT,\nID NUMBER DEFAULT 22 NOT NULL)");
    }

    @Test
    void buildAlterTableStatement() {
        Collection<Field> fields = List.of(
                new Field("helloWorld", 0, SchemaBuilder.string().optional().build()),
                new Field("id", 1, SchemaBuilder.int32().defaultValue(22).build())
        );

        String result = SnowflakeSql.buildAlterTableStatement(TABLE, fields);
        assertEquals(result, "ALTER TABLE \"DB1\".\"SCHEMA1\".\"TABLE1\" ADD\nHELLOWORLD TEXT,\nID NUMBER DEFAULT 22 NOT NULL");
    }

}