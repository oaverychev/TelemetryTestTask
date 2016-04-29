package storm.kafka;

import org.apache.storm.jdbc.common.ConnectionProvider;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

public class MemSqlConnectionProvider implements ConnectionProvider {
    private Map<String, Object> configMap;

    public MemSqlConnectionProvider(Map<String, Object> configMap) {
        this.configMap = configMap;
    }

    public void prepare() {
    }

    public  Connection getConnection() {
        Properties properties = new Properties();
        properties.putAll(configMap);
        Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3307/TelemetryDB",properties);
        } catch (SQLException ex) {
            System.out.println("SQLException: " + ex.getMessage());
            System.out.println("SQLState: " + ex.getSQLState());
            System.out.println("VendorError: " + ex.getErrorCode());
        }
        return conn;
    }

    public void cleanup() {
    }
}
