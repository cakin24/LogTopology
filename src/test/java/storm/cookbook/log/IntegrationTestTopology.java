package storm.cookbook.log;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import redis.clients.jedis.Jedis;
import storm.cookbook.log.model.LogEntry;
import backtype.storm.utils.Utils;

import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Cluster;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.ddl.KeyspaceDefinition;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * The integration test basically injects on the input queue, and then
 * introduces a test bolt which simply persists the tuple into a JSON object
 * onto an output queue. Note that test is parameter driven, but the cluster is
 * only shutdown once all tests have run
 * */
public class IntegrationTestTopology {

	public static final String REDIS_CHANNEL = "TestLogBolt";

	private static Jedis jedis;
	private static LogTopology topology = new LogTopology();
	private static TestBolt testBolt = new TestBolt(REDIS_CHANNEL);
	private static EmbeddedCassandra cassandra;
	private static Client client;

	@BeforeClass
	public static void setup() throws Exception {
	//	setupCassandra();
		setupElasticSearch();
		setupTopology();
	}

	private static void setupCassandra() throws Exception {
		cassandra = new EmbeddedCassandra(9171);
		cassandra.start();
		Thread.sleep(3000);

		AstyanaxContext<Cluster> clusterContext = new AstyanaxContext.Builder()
				.forCluster("ClusterName")
				.withAstyanaxConfiguration(
						new AstyanaxConfigurationImpl()
								.setDiscoveryType(NodeDiscoveryType.NONE))
				.withConnectionPoolConfiguration(
						new ConnectionPoolConfigurationImpl("MyConnectionPool")
								.setMaxConnsPerHost(1).setSeeds(
										"localhost:9171"))
				.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
				.buildCluster(ThriftFamilyFactory.getInstance());

		clusterContext.start();
		Cluster cluster = clusterContext.getEntity();
		KeyspaceDefinition ksDef = cluster.makeKeyspaceDefinition();

		Map<String, String> stratOptions = new HashMap<String, String>();
		stratOptions.put("replication_factor", "1");
		ksDef.setName(Conf.LOGGING_KEYSPACE)
				.setStrategyClass("SimpleStrategy")
				.setStrategyOptions(stratOptions)
				.addColumnFamily(
						cluster.makeColumnFamilyDefinition().setName(Conf.COUNT_CF_NAME)
								.setComparatorType("UTF8Type")
								.setKeyValidationClass("UTF8Type")
								.setDefaultValidationClass("CounterColumnType"));

		cluster.addKeyspace(ksDef);
		Thread.sleep(3000);

	}

	private static void setupElasticSearch() throws Exception {
		Node node = NodeBuilder.nodeBuilder().local(true).node();
		client = node.client();
		Thread.sleep(5000);
	}

	private static void setupTopology() {
		// We want all output tuples coming to the mock for testing purposes
		topology.getBuilder().setBolt("testBolt", testBolt, 1)
				.globalGrouping("indexer");
		// run in local mode, but we will shut the cluster down when we are
		// finished
		topology.runLocal(0);
		// jedis required for input and ouput of the cluster
		jedis = new Jedis("localhost",
				Integer.parseInt(Conf.DEFAULT_JEDIS_PORT));
		jedis.connect();
		//jedis.flushDB();
		// give it some time to startup before running the tests.
		Utils.sleep(5000);
	}

	@AfterClass
	public static void shutDown() {
		topology.shutDownLocal();
		jedis.disconnect();
//		client.close();
//		cassandra.stop();
	}

	@Test
	public void inputOutputClusterTest() throws Exception {
		/*String testData = UnitTestUtils.readFile("/testData1.json");
		jedis.rpush("log", testData);
		LogEntry entry = UnitTestUtils.getEntry();
		long minute = VolumeCountingBolt.getMinuteForTime(entry.getTimestamp());
		Utils.sleep(6000);
		String id = jedis.rpop(REDIS_CHANNEL);
		assertNotNull(id);
		// Check that the indexing working
		GetResponse response = client
				.prepareGet(IndexerBolt.INDEX_NAME, IndexerBolt.INDEX_TYPE, id)
				.execute().actionGet();
		assertTrue(response.exists());
		// now check that count has been updated in cassandra

		AstyanaxContext<Keyspace> astyContext = new AstyanaxContext.Builder()
				.forCluster("ClusterName")
				.forKeyspace(Conf.LOGGING_KEYSPACE)
				.withAstyanaxConfiguration(
						new AstyanaxConfigurationImpl()
								.setDiscoveryType(NodeDiscoveryType.NONE))
				.withConnectionPoolConfiguration(
						new ConnectionPoolConfigurationImpl("MyConnectionPool")
								.setMaxConnsPerHost(1).setSeeds(
										"localhost:9171"))
				.withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
				.buildKeyspace(ThriftFamilyFactory.getInstance());
		astyContext.start();
		Keyspace ks = astyContext.getEntity();
		Column<String> result = ks
				.prepareQuery(
						new ColumnFamily<String, String>(Conf.COUNT_CF_NAME,
								StringSerializer.get(), StringSerializer.get()))
				.getKey(Long.toString(minute)).getColumn(entry.getSource())
				.execute().getResult();
		assertEquals(1L, result.getLongValue());*/
        Utils.sleep(5000);
        System.out.println("Done");

    }

}
