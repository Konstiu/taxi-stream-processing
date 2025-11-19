package main.java.com.example;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import static main.java.com.example.TaxiTopology.SpeedBolt.FC_LAT;
import static main.java.com.example.TaxiTopology.SpeedBolt.FC_LON;
import static main.java.com.example.TaxiTopology.SpeedBolt.haversineMeters;

public class TaxiTopology {

    // ✅ Kafka Spout
    private static KafkaSpout<String, String> buildKafkaSpout(String brokers, String topic, String group) {
        KafkaSpoutConfig<String, String> cfg =
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

        @Override
        public void prepare(Map<String, Object> topoConf, org.apache.storm.task.TopologyContext ctx) {
            mapper = new ObjectMapper();
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector out) {
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

        @Override
        public void declareOutputFields(OutputFieldsDeclarer d) {
            d.declare(new Fields("taxiId", "ts", "lat", "lon"));
        }
    }

    // ✅ Redis Bolt
    public static class RedisBolt extends org.apache.storm.topology.base.BaseRichBolt {
        private transient org.apache.storm.task.OutputCollector collector;
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
        public void prepare(Map<String, Object> conf, org.apache.storm.task.TopologyContext ctx, org.apache.storm.task.OutputCollector collector) {
            this.collector = collector;
            this.pool = new JedisPool(new JedisPoolConfig(), host, port);
        }

        @Override
        public void execute(Tuple t) {
            try (Jedis jedis = pool.getResource()){
                String taxiId = t.getStringByField("taxiId");
                long ts = t.getLongByField("ts");
                double lat = t.getDoubleByField("lat");
                double lon = t.getDoubleByField("lon");
                double speedKmh = t.getFields().contains("speedKmh") ? t.getDoubleByField("speedKmh") : 0.0;
                double distMeters = t.getFields().contains("meters_since_last") ? t.getDoubleByField("meters_since_last") : 0.0;
                double avgSpeed   = t.getFields().contains("avgSpeedKmh") ? t.getDoubleByField("avgSpeedKmh") : 0.0;
                double distKm     = t.getFields().contains("distKm") ? t.getDoubleByField("distKm") : 0.0;
                boolean crossed10 = t.getFields().contains("crossed10km") && t.getBooleanByField("crossed10km");
                boolean outside15 = t.getFields().contains("outside15km") && t.getBooleanByField("outside15km");

                String key = "taxi:" + taxiId + ":state";

                // 1) raise one-time alert when crossing 10km
                if (crossed10) {
                    jedis.lpush("alerts", "left_area_10km,"+taxiId+","+ts+","+distKm);
                }
                // Running distance (inside 15 km only)
                if (!outside15 && distMeters > 0.0) {
                    jedis.hincrByFloat(key, "distance_km_total", distMeters / 1000.0);
                }

                // Speeding alert once (>50 km/h)
                if (speedKmh > 50.0 && jedis.setnx("alert:speed:"+taxiId, "1") == 1) {
                    jedis.lpush("alerts", "speeding,"+taxiId+","+ts+","+speedKmh);
                    jedis.expire("alert:speed:"+taxiId, 3600); // optional cooldown
                }
                // 2) store state (always), but control what the dashboard reads
                jedis.hset(key, Map.of(
                        "ts", String.valueOf(ts),
                        "lat", String.valueOf(lat),
                        "lon", String.valueOf(lon),
                        "speed_kmh", String.valueOf(speedKmh),
                        "avg_speed_kmh", String.valueOf(avgSpeed),
                        "meters_since_last", String.valueOf(distMeters),
                        "dist_km_center", String.valueOf(distKm),
                        "outside_15km", String.valueOf(outside15)
                ));
                if (ttl > 0) jedis.expire(key, ttl);

                // 3) geospatial set for map: only include if within 15km
                if (!outside15) {
                    jedis.geoadd("taxis:geo", lon, lat, taxiId);
                } else {
                    // optionally remove it if it was on the map before
                    jedis.zrem("taxis:geo", taxiId);
                }
                jedis.lpush("taxi:" + taxiId + ":track", ts + "," + lat + "," + lon + "," + speedKmh + "," + distMeters);
                jedis.ltrim("taxi:" + taxiId + ":track", 0, 99);
                collector.ack(t);
            } catch (Exception e) {
                collector.reportError(e);
                collector.fail(t);
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer d) {
        }
    }

    public static class AverageSpeedBolt extends BaseBasicBolt {
        private static class Stats {
            double sum; long count; boolean alerted10;
        }
        private transient Map<String, Stats> byTaxi;

        @Override
        public void prepare(Map<String, Object> topoConf, org.apache.storm.task.TopologyContext ctx) {
            this.byTaxi = new java.util.HashMap<>();
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector out) {
            String taxiId = input.getStringByField("taxiId");
            long   ts     = input.getLongByField("ts");
            double lat    = input.getDoubleByField("lat");
            double lon    = input.getDoubleByField("lon");
            double speedKmh = input.getDoubleByField("speedKmh");
            double meters   = input.getDoubleByField("meters_since_last");

            // distance from Forbidden City
            double dKm = haversineMeters(lat, lon, FC_LAT, FC_LON) / 1000.0;

            Stats st = byTaxi.computeIfAbsent(taxiId, k -> new Stats());
            boolean crossed10Now = false;

            // fire a one-time "crossed 10km" notification signal
            if (!st.alerted10 && dKm > 10.0) {
                st.alerted10 = true;
                crossed10Now = true;
            }

            // update avg only inside 15km
            if (dKm <= 15.0 && speedKmh > 0.0) {
                st.sum += speedKmh;
                st.count += 1;
            }
            double avg = (st.count == 0) ? 0.0 : (st.sum / st.count);

            boolean outside15 = dKm > 15.0;

            // emit downstream (RedisBolt will decide how to store)
            out.emit(new Values(taxiId, ts, lat, lon, speedKmh, meters, avg, dKm, crossed10Now, outside15));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer d) {
            d.declare(new Fields(
                    "taxiId","ts","lat","lon",
                    "speedKmh","meters_since_last",
                    "avgSpeedKmh","distKm",
                    "crossed10km","outside15km"
            ));
        }
    }




    // ✅ Compute speed between consecutive GPS fixes
    public static class SpeedBolt extends BaseBasicBolt {
        // previous state per taxiId (in-memory)
		private static final Logger LOG = LoggerFactory.getLogger(SpeedBolt.class);
        private static class Fix {
            final long ts;
            final double lat;
            final double lon;

            Fix(long ts, double lat, double lon) {
                this.ts = ts;
                this.lat = lat;
                this.lon = lon;
            }
        }

        private transient Map<String, Fix> last;

        @Override
        public void prepare(Map<String, Object> topoConf, org.apache.storm.task.TopologyContext ctx) {
            this.last = new java.util.HashMap<>();
        }

        @Override
        public void execute(Tuple input, BasicOutputCollector out) {
            String taxiId = input.getStringByField("taxiId");
            long ts = input.getLongByField("ts");
            double lat = input.getDoubleByField("lat");
            double lon = input.getDoubleByField("lon");

            double speedKmh = 0.0; // default when we don’t have a previous point
            Fix prev = this.last.get(taxiId);
			double distMeters = 0.0;

            if (prev != null) {
                long dtMillis = ts - prev.ts;
                if (dtMillis > 0) {
                    distMeters = haversineMeters(prev.lat, prev.lon, lat, lon); // distance
                    double dtHours = (dtMillis / 1000.0) / 3600.0;                      // time
                    double km = distMeters / 1000.0;
                    speedKmh = km / dtHours;

                    // simple sanity cap to filter out GPS jumps; tune as needed
                    //if (speedKmh < 0 || speedKmh > 200.0) speedKmh = 0.0;
                }
            }
            // update cache
            last.put(taxiId, new Fix(ts, lat, lon));

            // emit downstream with speed
            out.emit(new Values(taxiId, ts, lat, lon, speedKmh, distMeters));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer d) {
            d.declare(new Fields("taxiId", "ts", "lat", "lon", "speedKmh", "meters_since_last"));
        }

        // Forbidden City (Beijing) center
        public static final double FC_LAT = 39.9163;
        public static final double FC_LON = 116.3972;
        // ✅ Haversine distance in meters
        static double haversineMeters(double lat1, double lon1, double lat2, double lon2) {
            double R = 6378137; // Earth radius (m)
            double dLat = lat2 * Math.PI / 180 - lat1 * Math.PI / 180;
            double dLon = lon2 * Math.PI / 180 - lon1 * Math.PI / 180;
            double a = Math.sin(dLat/2) * Math.sin(dLat/2) +
				Math.cos(lat1 * Math.PI / 180) * Math.cos(lat2 * Math.PI / 180) *
				Math.sin(dLon/2) * Math.sin(dLon/2);
			double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
            return R * c;
        }


    }
    public static void main(String[] args) throws Exception {
        String brokers = System.getenv().getOrDefault("KAFKA_BROKERS", "broker:29092");
        String topic = System.getenv().getOrDefault("KAFKA_TOPIC", "taxi-locations");
        String group = System.getenv().getOrDefault("KAFKA_GROUP", "storm-taxi");
        String redisHost = System.getenv().getOrDefault("REDIS_HOST", "redis");
        int redisPort = Integer.parseInt(System.getenv().getOrDefault("REDIS_PORT", "6379"));
        int redisTtl = Integer.parseInt(System.getenv().getOrDefault("REDIS_TTL", "600"));

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-spout", TaxiTopology.buildKafkaSpout(brokers, topic, group), 1);
        builder.setBolt("parse-json", new TaxiTopology.ParseJsonBolt(), 2).shuffleGrouping("kafka-spout");
        builder.setBolt("compute-speed", new TaxiTopology.SpeedBolt(), 2).fieldsGrouping("parse-json", new Fields("taxiId"));
        builder.setBolt("avg-speed", new TaxiTopology.AverageSpeedBolt(), 2)
                .fieldsGrouping("compute-speed", new Fields("taxiId"));
        builder.setBolt("redis-store", new TaxiTopology.RedisBolt(redisHost, redisPort, redisTtl), 2)
                .fieldsGrouping("avg-speed", new Fields("taxiId"));


        Config config = new Config();
        config.setNumWorkers(1);
        config.setMessageTimeoutSecs(120);
        StormSubmitter.submitTopology("taxi-topo", config, builder.createTopology());
    }
}

