/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mmt.metrics.kafka.opentsdb;

import com.codahale.metrics.*;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

public class OpenTSDBKafkaReporter extends ScheduledReporter {
    private static final Logger log = LoggerFactory.getLogger(OpenTSDBKafkaReporter.class);

    private final Producer<String, String> kafkaProducer;
    private final String kafkaTopic;
    private final MetricRegistry registry;
    private final Map<String, String> tags;

    private OpenTSDBKafkaReporter(MetricRegistry registry,
                                  String name,
                                  MetricFilter filter,
                                  TimeUnit rateUnit,
                                  TimeUnit durationUnit,
                                  String kafkaTopic,
                                  Map<String, String> tags,
                                  Producer producer) {
        super(registry, name, filter, rateUnit, durationUnit);
        this.registry = registry;
        this.kafkaTopic = kafkaTopic;
        this.kafkaProducer = producer;
        this.tags = tags;
    }

    @Override
    public synchronized void report(SortedMap<String, Gauge> gauges,
                                    SortedMap<String, Counter> counters,
                                    SortedMap<String, Histogram> histograms,
                                    SortedMap<String, Meter> meters,
                                    SortedMap<String, Timer> timers) {
        try {
            long currentTime = System.currentTimeMillis();
            if (!gauges.isEmpty()) {
                for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                    sendMetricsToKafka(entry.getKey(), currentTime, entry);
                }
            }

            if (!counters.isEmpty()) {
                for (Map.Entry<String, Counter> entry : counters.entrySet()) {
                    sendCountersToKafka(entry.getKey(), currentTime, entry);
                }
            }
            if (!histograms.isEmpty()) {
                for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
                    sendHistogramToKafka(entry.getKey(), currentTime, entry.getValue());
                }
            }

            if (!meters.isEmpty()) {
                for (Map.Entry<String, Meter> entry : meters.entrySet()) {
                    sendMeterToKafka(entry.getKey(), currentTime, entry.getValue());
                }
            }

            if (!timers.isEmpty()) {
                for (Map.Entry<String, Timer> entry : timers.entrySet()) {
                    sendTimerToKafka(entry.getKey(), currentTime, entry.getValue());
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    public static Builder builder(MetricRegistry registry, String kafkaTopic, Producer producer) {
        return new Builder(registry, kafkaTopic, producer);
    }

    private void sendMeterToKafka(String key, long time, Meter meter) {
        String metric = getOpenTSDBMetric(key + ".rate.mean", time, convertRate(meter.getMeanRate()), null);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));

        metric = getOpenTSDBMetric(key + ".rate.1min", time, convertRate(meter.getOneMinuteRate()), null);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));

        metric = getOpenTSDBMetric(key + ".rate.5min", time, convertRate(meter.getFiveMinuteRate()), null);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));

        metric = getOpenTSDBMetric(key + ".rate.15min", time, convertRate(meter.getFifteenMinuteRate()), null);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));
    }

    private void sendCountersToKafka(String key, long time, Map.Entry<String, Counter> entry) {
        String metric = getOpenTSDBMetric(key + ".count", time, entry.getValue().getCount(), null);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));
    }

    private void sendMetricsToKafka(String key, long time, Map.Entry<String, Gauge> entry) {
        String metric = getOpenTSDBMetric(key + ".gauge", time, entry.getValue().getValue(), null);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));
    }

    private void sendHistogramToKafka(String key, long time, Histogram histogram) {
        Snapshot snapshot = histogram.getSnapshot();

        HashMap<String, String> tags = null;
        String metric = getOpenTSDBMetric(key + ".pc.min", time, snapshot.getMin(), tags);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));

        metric = getOpenTSDBMetric(key + ".pc.max", time, snapshot.getMax(), tags);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));

        metric = getOpenTSDBMetric(key + ".pc.mean", time, snapshot.getMean(), tags);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));

        metric = getOpenTSDBMetric(key + ".pc.stddev", time, snapshot.getStdDev(), tags);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));

        metric = getOpenTSDBMetric(key + ".pc.median", time, snapshot.getMedian(), tags);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));

        metric = getOpenTSDBMetric(key + ".pc.75-pc", time, snapshot.get75thPercentile(), tags);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));

        metric = getOpenTSDBMetric(key + ".pc.95-pc", time, snapshot.get95thPercentile(), tags);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));

        metric = getOpenTSDBMetric(key + ".pc.98-pc", time, snapshot.get98thPercentile(), tags);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));

        metric = getOpenTSDBMetric(key + ".pc.99-pc", time, snapshot.get99thPercentile(), tags);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));

        metric = getOpenTSDBMetric(key + ".pc.999-pc", time, snapshot.get999thPercentile(), tags);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));
    }

    private void sendTimerToKafka(String key, long time, Timer timer) {
        final Snapshot snapshot = timer.getSnapshot();
        HashMap<String, String> tags = null;
        String metric = getOpenTSDBMetric(key + ".timer.min", time, convertDuration(snapshot.getMin()), tags);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));

        metric = getOpenTSDBMetric(key + ".timer.mean", time, convertDuration(snapshot.getMean()), tags);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));

        metric = getOpenTSDBMetric(key + ".timer.stddev", time, convertDuration(snapshot.getStdDev()), tags);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));

        metric = getOpenTSDBMetric(key + ".timer.median", time, convertDuration(snapshot.getMedian()), tags);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));

        metric = getOpenTSDBMetric(key + ".timer.75-pc", time, convertDuration(snapshot.get75thPercentile()), tags);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));

        metric = getOpenTSDBMetric(key + ".timer.95-pc", time, convertDuration(snapshot.get95thPercentile()), tags);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));

        metric = getOpenTSDBMetric(key + ".timer.98-pc", time, convertDuration(snapshot.get98thPercentile()), tags);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));

        metric = getOpenTSDBMetric(key + ".timer.99-pc", time, convertDuration(snapshot.get99thPercentile()), tags);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));

        metric = getOpenTSDBMetric(key + ".timer.959-pc", time, convertDuration(snapshot.get999thPercentile()), tags);
        kafkaProducer.send(new KeyedMessage<String, String>(kafkaTopic, metric));
    }


    public static class Builder {
        private MetricRegistry registry;
        private String kafkaTopic;
        private Producer producer;
        private Map<String, String> tags;


        private String name = "OpenTSDBKafkaReporter";
        private MetricFilter filter = MetricFilter.ALL;
        private TimeUnit rateUnit = TimeUnit.SECONDS;
        private TimeUnit durationUnit = TimeUnit.SECONDS;

        public Builder(MetricRegistry registry, String topic, Producer producer) {
            this.registry = registry;
            this.kafkaTopic = topic;
            this.producer = producer;
        }

        public String getKafkaTopic() {
            return kafkaTopic;
        }

        public Builder setKafkaTopic(String kafkaTopic) {
            this.kafkaTopic = kafkaTopic;
            return this;
        }


        public MetricRegistry getRegistry() {
            return registry;
        }

        public Builder setRegistry(MetricRegistry registry) {
            this.registry = registry;
            return this;
        }

        public String getName() {
            return name;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public MetricFilter getFilter() {
            return filter;
        }

        public Builder setFilter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        public TimeUnit getRateUnit() {
            return rateUnit;
        }

        public Builder setRateUnit(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        public TimeUnit getDurationUnit() {
            return durationUnit;
        }

        public Builder setDurationUnit(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        public Map<String, String> getTags() {
            return tags;
        }

        public Builder setTags(Map<String, String> tags) {
            this.tags = tags;
            return this;
        }

        public OpenTSDBKafkaReporter build() {
            return new OpenTSDBKafkaReporter(registry, name, filter, rateUnit, durationUnit, kafkaTopic, tags, producer);
        }
    }

    public String getOpenTSDBMetric(String key, long timestamp, Object value, Map<String, String> extraTags) {
        StringBuilder builder = new StringBuilder();
        builder.append("put");
        builder.append(" ");
        builder.append(key);
        builder.append(" ");
        builder.append(timestamp);
        builder.append(" ");
        builder.append(value);
        builder.append(" ");
        if (tags != null) {
            for (String tagKey : tags.keySet()) {
                builder.append(tagKey);
                builder.append("=");
                builder.append(tags.get(tagKey));
                builder.append(" ");
            }
        }
        if (extraTags != null) {
            for (String tagKey : extraTags.keySet()) {
                builder.append(tagKey);
                builder.append("=");
                builder.append(extraTags.get(tagKey));
                builder.append(" ");
            }
        }
        return builder.toString();
    }
}