package storm.cookbook.log;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;

import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import com.hmsonline.storm.cassandra.StormCassandraConstants;
import com.hmsonline.storm.cassandra.bolt.AckStrategy;
import com.hmsonline.storm.cassandra.bolt.CassandraBatchingBolt;
import com.hmsonline.storm.cassandra.bolt.CassandraBolt;
import com.hmsonline.storm.cassandra.bolt.CassandraCounterBatchingBolt;
import com.hmsonline.storm.cassandra.bolt.mapper.DefaultTupleMapper;

import java.util.Arrays;
import java.util.HashMap;

public class LogTopology {

	private TopologyBuilder builder = new TopologyBuilder();
	private Config conf = new Config();
	private LocalCluster cluster;
    String configKey = "cassandra-config";
    HashMap<String, Object> clientConfig = new HashMap<String, Object>();

	public LogTopology() {
        clientConfig.put(StormCassandraConstants.CASSANDRA_HOST, "localhost:9160");
        clientConfig.put(StormCassandraConstants.CASSANDRA_KEYSPACE, Arrays.asList(new String[]{"Logging"}));
		builder.setSpout("logSpout", new LogSpout(), 1);

		builder.setBolt("logRules", new LogRulesBolt(), 1).shuffleGrouping(
				"logSpout");
		builder.setBolt("indexer", new IndexerBolt(), 1).shuffleGrouping(
				"logRules");
		builder.setBolt("counter", new VolumeCountingBolt(), 1).shuffleGrouping("logRules");

        CassandraBatchingBolt<String, String, String> logPersistenceBolt = new CassandraBatchingBolt<String, String, String>(configKey,
                new DefaultTupleMapper("Logging", "LogVolumeByMinute", VolumeCountingBolt.FIELD_ROW_KEY));
        logPersistenceBolt.setAckStrategy(AckStrategy.ACK_ON_RECEIVE);

        builder.setBolt("countPersistor", logPersistenceBolt, 1)
				.shuffleGrouping("counter");

		// Maybe add:
		// Stem and stop word counting per file
		// The persister for the stem analysis (need to check the counting
		// capability first on storm-cassandra)
		conf.put(Conf.REDIS_PORT_KEY, Conf.DEFAULT_JEDIS_PORT);
		conf.put(configKey, clientConfig);
	}

	public TopologyBuilder getBuilder() {
		return builder;
	}

	public LocalCluster getLocalCluster() {
		return cluster;
	}

	public Config getConf() {
		return conf;
	}

	public void runLocal(int runTime) {
		conf.setDebug(true);
		conf.put(Conf.REDIS_HOST_KEY, "localhost");
		conf.put(StormCassandraConstants.CASSANDRA_HOST, "localhost:9160");
		cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		if (runTime > 0) {
			Utils.sleep(runTime);
			shutDownLocal();
		}
	}

	public void shutDownLocal() {
		if (cluster != null) {
			cluster.killTopology("test");
			cluster.shutdown();
		}
	}

	public void runCluster(String name, String redisHost, String cassandraHost)
			throws AlreadyAliveException, InvalidTopologyException {
		conf.setNumWorkers(1);
		conf.put(Conf.REDIS_HOST_KEY, redisHost);
		conf.put(StormCassandraConstants.CASSANDRA_HOST,cassandraHost);
		StormSubmitter.submitTopology(name, conf, builder.createTopology());
	}

	public static void main(String[] args) throws Exception {

		LogTopology topology = new LogTopology();

		if (args != null && args.length > 1) {
			topology.runCluster(args[0], args[1], args[2]);
		} else {
			if (args != null && args.length == 1)
				System.out
						.println("Running in local mode, redis ip missing for cluster run");
			topology.runLocal(10000);
		}

	}

}
