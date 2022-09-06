package lawrencemq.SnowflakeSinkConnector.sql;


public final class ColumnDescription {
    private final String columnName;
    private final String typeName;
    private final int sqlType;
    private final boolean nullable;

    public ColumnDescription(String columnName, int sqlType, String typeName, boolean nullable) {
        this.columnName = columnName;
        this.sqlType = sqlType;
        this.typeName = typeName;
        this.nullable = nullable;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getTypeName() {
        return typeName;
    }
    public int getSqlType(){
        return sqlType;
    }

    public boolean isNullable(){
        return this.nullable;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ColumnDescription that = (ColumnDescription) o;

        if (sqlType != that.sqlType) return false;
        if (nullable != that.nullable) return false;
        if (!columnName.equals(that.columnName)) return false;
        return typeName.equals(that.typeName);
    }

    @Override
    public int hashCode() {
        int result = columnName.hashCode();
        result = 31 * result + typeName.hashCode();
        result = 31 * result + sqlType;
        result = 31 * result + (nullable ? 1 : 0);
        return result;
    }

    @Override
    public String toString() {
        return String.format("ColumnDescription[columnName=%s,typeName=%s,sqlType=%d,nullable=%s]",
                columnName, typeName, sqlType, nullable);
    }





}
