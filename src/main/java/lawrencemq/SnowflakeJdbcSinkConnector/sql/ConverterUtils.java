package lawrencemq.SnowflakeJdbcSinkConnector.sql;

import com.google.gson.Gson;

final class ConverterUtils {
    private static final Gson gson = new Gson();

    static String convertToJSON(Object value){
        return gson.toJson(value);
    }

}
