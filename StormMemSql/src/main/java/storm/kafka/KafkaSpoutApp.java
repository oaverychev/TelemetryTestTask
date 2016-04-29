package storm.kafka;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

public class KafkaSpoutApp {

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        ZkHosts zkHosts = new ZkHosts("127.0.0.1:2181");

        String topic_name = "alert";
        String consumer_group_id = "id7";
        String zookeeper_root = "";
        SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, topic_name, zookeeper_root, consumer_group_id);

        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), 1);
        builder.setBolt("IntermediateBolt", new IntermediateBolt()).shuffleGrouping("KafkaSpout");
        builder.setBolt("PersistenceBolt", new PersistenceBolt()).shuffleGrouping("IntermediateBolt");

        Config config = new Config();
        config.registerSerialization(LogMessage.class, LogMessageSerializer.class);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("KafkaConsumerTopology", config, builder.createTopology());

        try {
            Thread.sleep(60000);
        } catch (InterruptedException ex) {
            ex.printStackTrace();
        }

        cluster.killTopology("KafkaConsumerTopology");
        cluster.shutdown();
    }
}