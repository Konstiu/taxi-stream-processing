package com.example;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public class TaxiTopology {

  // Kafka spout configuration
  private static KafkaSpout<String, String> buildKafkaSpout(String brokers, String topic, String group) {
    KafkaSpoutConfig.Builder<String, String> builder =
        KafkaSpoutConfig.builder(brokers, topic)
          .setProp("group.id", group)
          .setProp("auto.offset.reset", "earliest")
          .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.EARLIEST)
          .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE);
    return new KafkaSpout<>(builder.build());
  }

  // Bolt 1: parse JSON
  public static class ParseJsonBolt extends BaseBasicBolt {
    private transient ObjectMapper mapper;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
      mapper = new ObjectMapper();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
      try {
        String key = input.getStringByField("key");
        String value = input.getStringByField("value");
        JsonNode json = mapper.readTree(value);

        String taxiId = key != null ? key : json.path("taxiId").asText(json.path("taxId").asText());
        long ts = json.path("ts").asLong();
        double lat = json.path("lat").asDouble();
        double lon = json.path("lon").asDouble();

        collector.emit(new org.apache.storm.tuple.Values(taxiId, ts, lat, lon));
      } catch (Exception ignored) {
        // skip malformed messages
      }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("taxiId", "ts", "lat", "lon"));
    }
  }

  // Bolt 2: store/update state in Redis
  public static class RedisBolt extends org.apache.storm.topology.base.BaseRichBolt {
    private transient OutputCollector collector;
    private transient JedisPool pool;
    private final String host;
    private final int port;
    private final int ttl;

    public RedisBolt(String host, int port, int ttl) {
      this.host = host;
      this.port = port;
      this.ttl = ttl;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext ctx, OutputCollector collector) {
      this.collector = collector;
      this.pool = new JedisPool(new JedisPoolConfig(), host, port);
    }

    @Override
    public void execute(Tuple input) {
      String taxiId = input.getStringByField("taxiId");
      long ts = input.getLongByField("ts");
      double lat = input.getDoubleByField("lat");
      double lon = input.getDoubleByField("lon");

      try (Jedis jedis = pool.getResource()) {
        String key = "taxi:" + taxiId + ":state";
        jedis.hset(key, Map.of(
            "ts", String.valueOf(ts),
            "lat", String.valueOf(lat),
            "lon", String.valueOf(lon)
        ));
        if (ttl > 0) jedis.expire(key, ttl);

        jedis.geoadd("taxis:geo", lon, lat, taxiId);
        jedis.lpush("taxi:" + taxiId + ":track", ts + "," + lat + "," + lon);
        jedis.ltrim("taxi:" + taxiId + ":track", 0, 99);
      }

      collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
  }

  public static void main(String[] args) throws Exception {
    String brokers = System.getenv().getOrDefault("KAFKA_BROKERS", "broker:29092");
    String topic   = System.getenv().getOrDefault("KAFKA_TOPIC", "taxi-locations");
    String group   = System.getenv().getOrDefault("KAFKA_GROUP", "storm-taxi");
    String redisHost = System.getenv().getOrDefault("REDIS_HOST", "redis");
    int redisPort = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
    int redisTtl = Integer.parseInt(System.getenv().getOrDefault("REDIS_TTL", "600"));

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("kafka-spout", buildKafkaSpout(brokers, topic, group), 1);
    builder.setBolt("parse-json", new ParseJsonBolt(), 2)
           .shuffleGrouping("kafka-spout");
    builder.setBolt("redis-store", new RedisBolt(redisHost, redisPort, redisTtl), 2)
           .fieldsGrouping("parse-json", new Fields("taxiId"));

    Config config = new Config();
    config.setNumWorkers(1);
    config.setMessageTimeoutSecs(60);

    StormSubmitter.submitTopology("taxi-topo", config, builder.createTopology());
  }
}
