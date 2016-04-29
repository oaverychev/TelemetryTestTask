package storm.kafka;

import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;
import org.apache.storm.jdbc.common.Util;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.shade.com.google.common.collect.Maps;
import org.apache.storm.tuple.ITuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


@SuppressWarnings("rawtypes")
public class MyJdbcUtils {

    private static class MemSqlJdbcMapper implements JdbcMapper {

        private List<Column> schemaColumns;

        public MemSqlJdbcMapper(String tableName, ConnectionProvider connectionProvider) {
            int queryTimeoutSecs = 30;
            JdbcClient client = new JdbcClient(connectionProvider, queryTimeoutSecs);
            this.schemaColumns = client.getColumnSchema(tableName);
        }

        public List<Column> getColumns(ITuple tuple) {
            ArrayList<Column> columns = new ArrayList<Column>();

            for(Column column : schemaColumns) {
                String columnName = column.getColumnName();
                Integer columnSqlType = column.getSqlType();
                LogMessage obj = (LogMessage) tuple.getValue(0);

                if(columnName.equals("name")) {
                    String value = obj.getName();
                    columns.add(new Column(columnName, value, columnSqlType));
                } else if(columnName.equals("type")) {
                    String value = obj.getType();
                    columns.add(new Column(columnName, value, columnSqlType));
                } else if(columnName.equals("count_json")) {
                    Integer value = obj.getCount();
                    columns.add(new Column(columnName, value, columnSqlType));
                } else if(columnName.equals("timestamp")) {
                    long value = obj.getUnixTimestamp();
                    columns.add(new Column(columnName, value, columnSqlType));
                } else {
                    throw new RuntimeException("Unsupported java type in tuple " + Util.getJavaType(columnSqlType));
                }
            }
            return columns;
        }

    }

    private static final ConnectionProvider CP;
    private static final JdbcMapper MAPPER;
    public static final String INSERT_STMT ="INSERT INTO alerts (timestamp,name,type,count_json) values (?,?,?,?)";


    static {

        Map configMap = Maps.newHashMap();
        configMap.put("dataSourceClassName","com.mysql.jdbc.jdbc2.optional.MysqlDataSource");
        configMap.put("url", "jdbc:mysql://localhost/TelemetryDB");
        configMap.put("user","root");
        configMap.put("password","root");
        CP = new MemSqlConnectionProvider(configMap);

        String tableName = "alerts";
        MAPPER = new MemSqlJdbcMapper(tableName, CP);
    }

    public static ConnectionProvider getConnectionProvider() {
        return CP;
    }

    public static JdbcMapper getJdbcMapper() {
        return MAPPER;
    }

    public static String getInsertStmt() {
        return INSERT_STMT;
    }
}