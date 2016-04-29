package storm.kafka;

import org.apache.storm.jdbc.bolt.JdbcInsertBolt;

public class PersistenceBolt extends JdbcInsertBolt {

    public PersistenceBolt() {
        super(MyJdbcUtils.getConnectionProvider(),
                MyJdbcUtils.getJdbcMapper());
        withInsertQuery(MyJdbcUtils.getInsertStmt());
    }

}