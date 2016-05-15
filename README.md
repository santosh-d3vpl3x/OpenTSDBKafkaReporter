# OpenTSDBKafkaReporter
Streams OpenTSDB ascii formatted metrics to Kafka.
Flow[Codebase covers parts in bold]:

**metrics -> `put metric 100000000 10 tagk=tagv`-> Kafka** -> Stream processor -> OpenTSDB

## Configuring OpenTSDB Kafka Reporter
Configuration of reporter is easy.

    OpenTSDBKafkaReporter.Builder reporterBuilder = OpenTSDBKafkaReporter.builder(couchbase, "topic_name", configured_kafka_producer);
          Map<String, String> tags = new HashMap<String, String>();
          tags.put("cluster", "cb1");
          tags.put("bucket", "test");
          OpenTSDBKafkaReporter reporter = reporterBuilder.setDurationUnit(TimeUnit.MILLISECONDS)
                  .setRateUnit(TimeUnit.SECONDS).setTags(tags).build();
          reporter.start(1, TimeUnit.MINUTES);
          
For detailed example, please have a look at GettingStarter.java. It should elaborate the capabilities of metrics. 

Note: This is just a bridge between metrics 3.1.0 and OpenTSDB-Kafka. One should still follow good practices applicable metrics library and Kafka.
Addition of unit test cases for Kafka producer involves spinning up complete zk and kafka instances, verification etc. Deferred for some other time.
