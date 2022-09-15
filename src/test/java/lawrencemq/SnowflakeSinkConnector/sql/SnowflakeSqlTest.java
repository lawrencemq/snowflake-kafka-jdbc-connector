package lawrencemq.SnowflakeSinkConnector.sql;

import lawrencemq.SnowflakeSinkConnector.sink.KafkaColumnMetadata;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SnowflakeSqlTest {

    private static Table TABLE = new Table("db1", "schema1", "table1");


       @Test
    void buildInsertStatement() {
        List<KafkaColumnMetadata> columnsFromKafka = List.of(
                new KafkaColumnMetadata("robot_id", SchemaBuilder.INT16_SCHEMA),
                new KafkaColumnMetadata("robot_loc", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build())
        );

        String result = SnowflakeSql.buildInsertStatement(TABLE, columnsFromKafka);
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
        // todo make sure this is a valid command
    }

    @Test
    void buildAlterTableStatement() {
        Collection<Schema> fields = List.of(
                SchemaBuilder.string().name("helloWorld").optional().build(),
                SchemaBuilder.int32().name("id").defaultValue(22).build()
        );

        String result = SnowflakeSql.buildAlterTableStatement(TABLE, fields);
        assertEquals(result, "ALTER TABLE \"DB1\".\"SCHEMA1\".\"TABLE1\" \nADD HELLOWORLD TEXT,\nADD ID NUMBER DEFAULT 22 NOT NULL");
        // TODO MAKE SURE THAT THIS IS A VALID COMMAND.
    }

}