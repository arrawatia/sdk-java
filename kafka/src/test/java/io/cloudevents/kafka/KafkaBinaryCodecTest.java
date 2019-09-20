//package io.cloudevents.kafka;
//
//import io.cloudevents.SpecVersion;
//import io.cloudevents.kafka.v03.BinaryCodec;
//import io.cloudevents.v03.CloudEvent;
//import io.cloudevents.v03.CloudEventBuilder;
//import io.debezium.kafka.KafkaCluster;
//import io.debezium.util.Testing;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.consumer.KafkaConsumer;
//import org.apache.kafka.clients.consumer.OffsetResetStrategy;
//import org.apache.kafka.clients.producer.KafkaProducer;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.common.header.Headers;
//import org.apache.kafka.common.serialization.ByteArrayDeserializer;
//import org.apache.kafka.common.serialization.ByteArraySerializer;
//import org.apache.kafka.common.serialization.Serdes;
//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.BeforeEach;
//import org.junit.jupiter.api.Test;
//
//import java.io.File;
//import java.net.URI;
//import java.nio.charset.StandardCharsets;
//import java.time.Duration;
//import java.util.Arrays;
//import java.util.Properties;
//import java.util.UUID;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.TimeUnit;
//
//import static org.assertj.core.api.Assertions.assertThat;
//
//public class KafkaBinaryCodecTest {
//
//    private KafkaCluster kafkaCluster;
//    private File dataDir;
//    private KafkaTransportHeaders kafkaTransportHeaders = new KafkaTransportHeaders();
//
//    @BeforeEach
//    public void beforeEach() {
//        dataDir = Testing.Files.createTestingDirectory("cluster");
//        kafkaCluster = new KafkaCluster().usingDirectory(dataDir)
//                .deleteDataPriorToStartup(true)
//                .deleteDataUponShutdown(true)
//                .withPorts(2181, 9092);
//    }
//
//    @AfterEach
//    public void afterEach() {
//        kafkaCluster.shutdown();
//        Testing.Files.delete(dataDir);
//    }
//
//    @Test
//    public void testSendingCloudEvent() throws Exception {
//
//        final String topicName = "topicA";
//        KafkaConsumer consumer;
//        KafkaProducer producer;
//
//        // Start a cluster and create a topic ...
//        kafkaCluster.addBrokers(1).startup();
////        kafkaCluster.createTopics(topicName);
//
//        final Properties consumerProperties = kafkaCluster.useTo().getConsumerProperties(topicName, topicName, OffsetResetStrategy.EARLIEST);
//        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
//        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
//        consumer = new KafkaConsumer(consumerProperties);
//        consumer.subscribe(Arrays.asList(topicName));
//
//        final Properties producerProperties = kafkaCluster.useTo().getProducerProperties(topicName);
//        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
//        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
//        producer = new KafkaProducer<>(producerProperties);
//
//
//        final CloudEvent<String> cloudEvent = new CloudEventBuilder<String>()
//                .withType("My.Type")
//                .withId(UUID.randomUUID().toString())
//                .withSource(URI.create("/foo"))
//                .withData("Hello")
//                .withDatacontenttype("string")
//                .build();
//        System.out.println(cloudEvent);
//
//
//        BinaryCodec<String> codec = new BinaryCodec<String>(new Serdes.StringSerde());
//        producer.send(codec.encode(cloudEvent, topicName));
//
//        final CountDownLatch latch = new CountDownLatch(1);
//        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(1_000));
//        records.forEach(rec -> {
//
//            // raw Kafka stuff
//            Headers receivedHeaders = rec.headers();
//            assertThat(receivedHeaders.lastHeader(kafkaTransportHeaders.typeKey()).value()).isEqualTo("My.Type".getBytes(StandardCharsets.UTF_8));
//
//
//            // converted:
//            final CloudEvent<String> receivedCloudEvent = codec.decode(rec);
//            System.out.println(receivedCloudEvent);
//            assertThat(receivedCloudEvent.getType()).isEqualTo("My.Type");
//            assertThat(receivedCloudEvent.getSpecversion()).isEqualTo(SpecVersion.V_03.toString());
//            assertThat(receivedCloudEvent.getDatacontenttype().get()).isEqualTo("string");
//
//            assertThat(receivedCloudEvent.getData().isPresent()).isTrue();
//            assertThat(receivedCloudEvent.getData().get().equals("Hello"));
//            latch.countDown();
//        });
//
//        final boolean done = latch.await(2, TimeUnit.SECONDS);
//        assertThat(done).isTrue();
//    }
//}