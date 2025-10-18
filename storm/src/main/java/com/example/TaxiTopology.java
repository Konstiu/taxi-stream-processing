package com.example;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

public class TaxiTopology {

  // ✅ Kafka Spout
  private static KafkaSpout<String, String> buildKafkaSpout(String brokers, String topic, String group) {
    KafkaSpoutConfig<String,String> cfg =
        KafkaSpoutConfig.builder(brokers, topic)
          .setProp("group.id", group)
          .setProp("key.deserializer", StringDeserializer.class)
          .setProp("value.deserializer", StringDeserializer.class)
          .setProp("auto.offset.reset", "earliest")
          .setEmitNullTuples(false)
          .setRecordTranslator(new ByTopicRecordTranslator<>(
              r -> new Values(r.value()),
              new Fields("value")))
          .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_LEAST_ONCE)
          .build();
    return new KafkaSpout<>(cfg);
  }

  // ✅ Parse JSON Bolt
  public static class ParseJsonBolt extends BaseBasicBolt {
    private transient ObjectMapper mapper;
    @Override public void prepare(Map<String,Object> topoConf, org.apache.storm.task.TopologyContext ctx) {
      mapper = new ObjectMapper();
    }
    @Override public void execute(Tuple input, BasicOutputCollector out) {
      try {
        String raw = input.getStringByField("value");
        JsonNode n = mapper.readTree(raw);
        String taxiId = n.path("taxiId").asText();
        long ts = n.path("ts").asLong();
        double lat = n.path("lat").asDouble();
        double lon = n.path("lon").asDouble();
        out.emit(new Values(taxiId, ts, lat, lon));
      } catch (Exception e) {
        System.err.println("ParseJsonBolt error: " + e.getMessage());
      }
    }
    @Override public void declareOutputFields(OutputFieldsDeclarer d) {
      d.declare(new Fields("taxiId","ts","lat","lon"));
    }
  }

  // ✅ Redis Bolt
  public static class RedisBolt extends org.apache.storm.topology.base.BaseRichBolt {
    private transient org.apache.storm.task.OutputCollector collector;
    private transient JedisPool pool;
    private final String host; private final int port; private final int ttl;
    public RedisBolt(String host, int port, int ttl) {
      this.host = host; this.port = port; this.ttl = ttl;
    }
    @Override public void prepare(Map<String,Object> conf, org.apache.storm.task.TopologyContext ctx,
                                  org.apache.storm.task.OutputCollector collector) {
      this.collector = collector;
      this.pool = new JedisPool(new JedisPoolConfig(), host, port);
    }
    @Override public void execute(Tuple t) {
      try (Jedis jedis = pool.getResource()) {
        String taxiId = t.getStringByField("taxiId");
        long ts = t.getLongByField("ts");
        double lat = t.getDoubleByField("lat");
        double lon = t.getDoubleByField("lon");
        String key = "taxi:" + taxiId + ":state";
        jedis.hset(key, Map.of("ts", String.valueOf(ts), "lat", String.valueOf(lat), "lon", String.valueOf(lon)));
        if (ttl > 0) jedis.expire(key, ttl);
        jedis.geoadd("taxis:geo", lon, lat, taxiId);
        jedis.lpush("taxi:" + taxiId + ":track", ts + "," + lat + "," + lon);
        jedis.ltrim("taxi:" + taxiId + ":track", 0, 99);
        collector.ack(t);
      } catch (Exception e) {
        collector.reportError(e);
        collector.fail(t);
      }
    }
    @Override public void declareOutputFields(OutputFieldsDeclarer d) {}
  }

  // ✅ Topology Entry Point
  public static void main(String[] args) throws Exception {
    String brokers   = System.getenv().getOrDefault("KAFKA_BROKERS", "broker:29092");
    String topic     = System.getenv().getOrDefault("KAFKA_TOPIC", "taxi-locations");
    String group     = System.getenv().getOrDefault("KAFKA_GROUP", "storm-taxi");
    String redisHost = System.getenv().getOrDefault("REDIS_HOST", "redis");
    int redisPort    = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
    int redisTtl     = Integer.parseInt(System.getenv().getOrDefault("REDIS_TTL", "600"));

    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("kafka-spout", buildKafkaSpout(brokers, topic, group), 1);
    builder.setBolt("parse-json", new ParseJsonBolt(), 2).shuffleGrouping("kafka-spout");
    builder.setBolt("redis-store", new RedisBolt(redisHost, redisPort, redisTtl), 2)
           .fieldsGrouping("parse-json", new Fields("taxiId"));

    Config config = new Config();
    config.setNumWorkers(1);
    config.setMessageTimeoutSecs(120);
    StormSubmitter.submitTopology("taxi-topo", config, builder.createTopology());
  }
}
