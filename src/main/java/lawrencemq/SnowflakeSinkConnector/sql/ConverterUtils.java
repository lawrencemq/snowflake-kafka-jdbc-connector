package lawrencemq.SnowflakeSinkConnector.sql;

import com.google.gson.Gson;

class ConverterUtils {
    private static final Gson gson = new Gson();

    static String convertToJSON(Object value){
        return gson.toJson(value);
    }

}
