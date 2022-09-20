package lawrencemq.SnowflakeSinkConnector.sql;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class QueryBuilderTest {

    @Test
    void appendColumnsSpec() {
        List<Schema> integerSchemas = List.of(
                SchemaBuilder.int8().name("int8").build(),
                SchemaBuilder.int16().name("int16").build(),
                SchemaBuilder.int32().name("int32").optional().build(),
                SchemaBuilder.int64().name("int64").optional().build()
        );

        // Setting individually
        QueryBuilder queryBuilder = QueryBuilder.newQuery();
        for(Schema intSchema : integerSchemas){
            queryBuilder.appendColumnSpec(intSchema);
            queryBuilder.append(',');
            queryBuilder.appendNewLine();
        }
        String queryStr = queryBuilder
                .toString();

        assertEquals(queryStr, "INT8 NUMBER NOT NULL,\n" +
                "INT16 NUMBER NOT NULL,\n" +
                "INT32 NUMBER,\n" +
                "INT64 NUMBER,\n");

        // Setting all at once
        String queryStr2 = QueryBuilder.newQuery().appendColumnsSpec(integerSchemas).toString();
        assertEquals(queryStr2, "\nINT8 NUMBER NOT NULL,\n" +
                "INT16 NUMBER NOT NULL,\n" +
                "INT32 NUMBER,\n" +
                "INT64 NUMBER");
    }

    @Test
    void appendColumns() {

        LinkedHashMap<String, Schema> fieldToSchemaMap = new LinkedHashMap<>();
        fieldToSchemaMap.put("floatName", SchemaBuilder.float32().name("floatName").defaultValue(42.69F).build());
        fieldToSchemaMap.put("strName", SchemaBuilder.string().name("strName").optional().build());

        // Adding all at once
        String result = QueryBuilder.newQuery()
                .appendColumns(fieldToSchemaMap)
                .toString();

        assertEquals(result, "\nFLOATNAME,\nSTRNAME");

        // Adding one at a time
        String result2 = QueryBuilder.newQuery()
                .appendColumnName("floatName")
                .append(", ")
                .appendColumnName("strName")
                .toString();
        assertEquals(result2, "FLOATNAME, STRNAME");

    }

    @Test
    void appendStringQuoted() {
        String queryStr = QueryBuilder.newQuery()
                .append("SELECT * FROM \"TABLE\" WHERE A = ")
                .appendStringQuoted("test")
                .toString();
        assertEquals(queryStr, "SELECT * FROM \"TABLE\" WHERE A = 'test'");
    }

    @Test
    void appendTableName() {
        Table table = new Table("testy", "mc", "test_face");
        String queryStr = QueryBuilder.newQuery()
                .append("SELECT * FROM ")
                .appendTableName(table)
                .toString();
        assertEquals(queryStr, "SELECT * FROM \"TESTY\".\"MC\".\"TEST_FACE\"");
    }

    @Test
    void appendBinary() {
        String queryStr = QueryBuilder.newQuery()
                .append("SELECT 1 ")
                .append("WHERE HAM = ")
                .appendBinary("HAMED".getBytes())
                .toString();
        assertEquals(queryStr, "SELECT 1 WHERE HAM = x'48414D4544'");
    }

    @Test
    void appendNewLine() {
        String queryStr = QueryBuilder.newQuery()
                .append("SELECT 1")
                .appendNewLine()
                .append("WHERE 1=1")
                .toString();
        assertEquals(queryStr, "SELECT 1\nWHERE 1=1");
    }

    @Test
    void append() {
        String queryStr = QueryBuilder.newQuery()
                .append("SELECT 1 ")
                .append("WHERE 1=1")
                .toString();
        assertEquals(queryStr, "SELECT 1 WHERE 1=1");
    }

    @Test
    void appendList() {
        List<Integer> integerList = List.of(1, 2, 3, 4, 5);
        QueryBuilder queryBuilder = QueryBuilder.newQuery();

        QueryBuilder.Transform<Integer> transform = (i) -> {
            queryBuilder.append(i*2);
        };


        String queryStr = queryBuilder
                .append("SELECT ")
                .appendList()
                .withTransformation(transform)
                .withItems(integerList)
                .toString();
        assertEquals(queryStr, "SELECT 2,4,6,8,10");

    }
}