import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.*;
import com.mmt.metrics.kafka.opentsdb.OpenTSDBKafkaReporter;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import java.sql.Time;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class GetStarted {
    static final MetricRegistry couchbase = new MetricRegistry();
    static final MetricRegistry metrics = new MetricRegistry();

    public static void main(String args[]) {
        startReport();
        Meter requests = couchbase.meter("latency");
        couchbase.register(MetricRegistry.name("memory"), new MemoryUsageGaugeSet());
        couchbase.register(MetricRegistry.name("fd"), new FileDescriptorRatioGauge());
        couchbase.register(MetricRegistry.name("clg"), new ClassLoadingGaugeSet());
        couchbase.register(MetricRegistry.name("ctg"), new CachedThreadStatesGaugeSet(10, TimeUnit.SECONDS));
        couchbase.register(MetricRegistry.name("gc"), new GarbageCollectorMetricSet());
        couchbase.register(MetricRegistry.name("space"), new Gauge<Integer>() {
            public Integer getValue() {
                return 5;
            }
        });
        requests.mark();
        wait5Seconds();
    }

    static void startReport() {
        Properties props = new Properties();
        props.put("metadata.broker.list", "10.96.23.78:9092");
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        props.put("producer.type", "sync");
        Producer<String, String> kafkaProducer = new Producer<String, String>(new ProducerConfig(props));
        OpenTSDBKafkaReporter.Builder reporterBuilder = OpenTSDBKafkaReporter.builder(couchbase, "storm_trooper", kafkaProducer);
        Map<String, String> tags = new HashMap<String, String>();
        tags.put("cluster", "cb1");
        tags.put("bucket", "test");
        OpenTSDBKafkaReporter reporter = reporterBuilder.setDurationUnit(TimeUnit.MILLISECONDS)
                .setRateUnit(TimeUnit.SECONDS).setTags(tags).build();
        reporter.start(1, TimeUnit.SECONDS);

        ConsoleReporter reporter1 = ConsoleReporter.forRegistry(couchbase)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build();
        reporter1.start(1, TimeUnit.SECONDS);
    }

    static void wait5Seconds() {
        try {
            Thread.sleep(5 * 1000);
        } catch (InterruptedException e) {
        }
    }
}