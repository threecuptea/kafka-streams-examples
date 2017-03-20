package org.freemind.kafka.streams.utils;

/**
 * Try to get embedded kafka server 0.10.2.0 work for integration test without using Kafka internal scala class
 *
 * Port and modify from
 * https://github.com/confluentinc/examples/blob/3.2.x/kafka-streams/src/test/java/io/confluent/examples/streams/kafka/EmbeddedSingleNodeKafkaCluster.java
 *
 */

import kafka.server.KafkaConfig$;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Runs an in-memory, "embedded" Kafka cluster with 1 ZooKeeper instance and 1 Kafka broker.
 */
public class EmbeddedKafkaServer extends ExternalResource {

    private static final Logger log = LoggerFactory.getLogger(EmbeddedKafkaServer.class);
    private static final int DEFAULT_BROKER_PORT = 0; // 0 results in a random port being sel


    private EmbeddedZookeeper zookeeper;
    private KafkaEmbedded broker;
    private final Properties brokerConfig;


    /**
     * Creates and starts a Kafka cluster.
     */
    public EmbeddedKafkaServer() {
        this(new Properties());
    }

    public EmbeddedKafkaServer(Properties brokerConfig) {
        this.brokerConfig = brokerConfig;
    }

    public void start() throws Exception {
        log.debug("Initiating embedded Kafka server startup");
        log.debug("Starting a ZooKeeper instance...");
        zookeeper = new EmbeddedZookeeper();
        zookeeper.startup();
        log.debug("ZooKeeper instance is running at {}", zookeeper.connectString());

        Properties effectiveBrokerConfig = effectiveBrokerConfigFrom(brokerConfig, zookeeper);
        log.debug("Starting a Kafka instance on port {} ...",
                effectiveBrokerConfig.getProperty(KafkaConfig$.MODULE$.PortProp()));
        broker = new KafkaEmbedded(effectiveBrokerConfig);
        log.debug("Kafka instance is running at {}, connected to ZooKeeper at {}",
                broker.brokerList(), broker.zookeeperConnect());
    }

    private Properties effectiveBrokerConfigFrom(Properties brokerConfig, EmbeddedZookeeper zookeeper) {
        Properties effectiveConfig = new Properties();
        effectiveConfig.putAll(brokerConfig);
        effectiveConfig.put(KafkaConfig$.MODULE$.ZkConnectProp(), zookeeper.connectString());
        effectiveConfig.put(KafkaConfig$.MODULE$.PortProp(), DEFAULT_BROKER_PORT);
        effectiveConfig.put(KafkaConfig$.MODULE$.DeleteTopicEnableProp(), true);
        effectiveConfig.put(KafkaConfig$.MODULE$.LogCleanerDedupeBufferSizeProp(), 2 * 1024 * 1024L);
        effectiveConfig.put(KafkaConfig$.MODULE$.GroupMinSessionTimeoutMsProp(), 0);
        return effectiveConfig;
    }

    @Override
    protected void before() throws Exception {
        start();
    }

    @Override
    protected void after() {
        stop();
    }

    /**
     * Stop the Kafka server.
     */
    public void stop() {
        if (broker != null) {
            broker.stop();
        }
        if (zookeeper != null) {
            zookeeper.shutdown();
        }
    }

    /**
     * This cluster's `bootstrap.servers` value.  Example: `127.0.0.1:9092`.
     *
     * You can use this to tell Kafka producers how to connect to this cluster.
     */
    public String bootstrapServers() {
        return broker.brokerList();
    }

    /**
     * This cluster's ZK connection string aka `zookeeper.connect` in `hostnameOrIp:port` format.
     * Example: `127.0.0.1:2181`.
     *
     * You can use this to e.g. tell Kafka consumers how to connect to this cluster.
     */
    public String zookeeperConnect() {
        return zookeeper.connectString();
    }

    /**
     * Create a Kafka topic with 1 partition and a replication factor of 1.
     *
     * @param topic The name of the topic.
     */
    public void createTopic(String topic) {
        createTopic(topic, 1, 1, new Properties());
    }

    /**
     * Create a Kafka topic with the given parameters.
     *
     * @param topic       The name of the topic.
     * @param partitions  The number of partitions for this topic.
     * @param replication The replication factor for (the partitions of) this topic.
     */
    public void createTopic(String topic, int partitions, int replication) {
        createTopic(topic, partitions, replication, new Properties());
    }

    /**
     * Create a Kafka topic with the given parameters.
     *
     * @param topic       The name of the topic.
     * @param partitions  The number of partitions for this topic.
     * @param replication The replication factor for (partitions of) this topic.
     * @param topicConfig Additional topic-level configuration settings.
     */
    public void createTopic(String topic,
                            int partitions,
                            int replication,
                            Properties topicConfig) {
        broker.createTopic(topic, partitions, replication, topicConfig);
    }


}
