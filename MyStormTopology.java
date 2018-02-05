package com.storm.stream;

import java.util.UUID;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.FileSizeRotationPolicy.Units;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Sample Storm Topology that ingests sample CSV data from a Kafka topic, and
 * stores it in HDFS directory.
 */

//com.storm.stream.MyStormTopology
public class MyStormTopology {

	public static void main(String[] args)
			throws AlreadyAliveException, InvalidTopologyException, InterruptedException, AuthorizationException {

		// This class expects 3 arguments in this order:
		// HDFS Host, Kafka Topic, HDFS Output Directory
		/*if (args.length != 3) {
			System.out.println("Incorrect number of input arguments!");
			System.exit(1);
		}*/
		//String hostname = "andani.sakha.com:2181,pooja.sakha.com:2181,shyam.sakha.com:2181";
		String hostname = "localhost";
		String kafkaTopic = "thirdtest";
		String hdfsOutputDir ="/foo/";
		String hostname1 = "localhost";

		

		// Create an instance of HDFSBolt and initialize it

		// Sync with FileSystem after every 100 tuples.
		SyncPolicy syncPolicy = new CountSyncPolicy(10);

		// Rotate files after each 127MB
		FileRotationPolicy rotationPolicy = new FileSizeRotationPolicy(127.0f, Units.MB);

		// Input file is a CSV file
		RecordFormat format = new DelimitedRecordFormat().withFieldDelimiter(",");

		// Files in HDFS will be stored at path ‘hdfsOutputDir’ having
		// ‘test-*.csv' format
		FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPrefix("test-").withExtension(".csv")
				.withPath(hdfsOutputDir);
		HdfsBolt hdfsbolt = new HdfsBolt().withFsUrl("hdfs://" + hostname1 + ":9000").withFileNameFormat(fileNameFormat)
				.withRecordFormat(format).withRotationPolicy(rotationPolicy).withSyncPolicy(syncPolicy);

		// Create an instance of KafkaSpout and initialize it
		BrokerHosts hosts = new ZkHosts(hostname);
		SpoutConfig spoutConfig = new SpoutConfig(hosts, kafkaTopic, "/" + kafkaTopic, UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);

		// Create an instance of TopologyBuilder
		TopologyBuilder builder = new TopologyBuilder();

		// Set spout and bolt for this topology. Send output of kafka-spout as
		// input to hdfs-bolt.
		builder.setSpout("kafka-spout", kafkaSpout, 1).setNumTasks(1);
		builder.setBolt("hdfs-bolt", hdfsbolt, 1).setNumTasks(1).shuffleGrouping("kafka-spout");

		// Submit the topology to Storm cluster in distributed mode with name
		// "test-topology"
		Config conf = new Config();
		LocalCluster cluster = new LocalCluster();
		//conf.setNumWorkers(1);
		  //LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", conf, builder.createTopology());
		 // Utils.sleep(10000); cluster.killTopology("test"); cluster.shutdown();
		 
		//conf.setNumWorkers(1);
		//StormSubmitter.submitTopology("test-topology", conf, builder.createTopology());
	}
}