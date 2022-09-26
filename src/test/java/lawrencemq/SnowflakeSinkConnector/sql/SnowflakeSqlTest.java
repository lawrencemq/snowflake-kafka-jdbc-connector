package lawrencemq.SnowflakeSinkConnector.sql;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

class SnowflakeSqlTest {

    private static Table TABLE = new Table("db1", "schema1", "table1");


    @Test
    void buildInsertStatement() {
        LinkedHashMap<String, Schema> fieldsToSchemaMap = new LinkedHashMap<>();
        fieldsToSchemaMap.put("robot_id", SchemaBuilder.INT16_SCHEMA);
        fieldsToSchemaMap.put("robot_loc", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build());

        String result = SnowflakeSql.buildInsertStatement(TABLE, fieldsToSchemaMap);
        assertEquals(result, "INSERT INTO \"DB1\".\"SCHEMA1\".\"TABLE1\"(\nROBOT_ID,\nROBOT_LOC) SELECT ?,parse_json(?)\n");

    }

    @Test
    void buildCreateTableStatement() {

        Collection<Schema> fields = List.of(
                SchemaBuilder.string().name("helloWorld").optional().build(),
                SchemaBuilder.int32().name("id").defaultValue(22).build()
        );
        String result = SnowflakeSql.buildCreateTableStatement(TABLE, fields);
        assertEquals(result, "CREATE TABLE \"DB1\".\"SCHEMA1\".\"TABLE1\" (\nHELLOWORLD TEXT,\nID NUMBER DEFAULT 22 NOT NULL)");
    }

    @Test
    void buildAlterTableStatement() {
        Collection<Schema> fields = List.of(
                SchemaBuilder.string().name("helloWorld").optional().build(),
                SchemaBuilder.int32().name("id").defaultValue(22).build()
        );

        String result = SnowflakeSql.buildAlterTableStatement(TABLE, fields);
        assertEquals(result, "ALTER TABLE \"DB1\".\"SCHEMA1\".\"TABLE1\" ADD\nHELLOWORLD TEXT,\nID NUMBER DEFAULT 22 NOT NULL");
    }

}