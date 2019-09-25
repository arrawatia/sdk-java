package io.cloudevents.kafka.integration;

import io.cloudevents.SpecVersion;
import io.cloudevents.kafka.KafkaCloudEvents;
import io.cloudevents.kafka.KafkaTransportHeaders;
import io.cloudevents.kafka.v03.KafkaCloudEventsImpl;
import io.cloudevents.v03.CloudEvent;
import io.cloudevents.v03.CloudEventBuilder;
import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static io.cloudevents.kafka.KafkaCloudEvents.CONTENT_TYPE;
import static io.cloudevents.kafka.KafkaTransportHeaders.getHeaderIfExists;
import static io.cloudevents.kafka.KafkaTransportHeaders.getRequiredHeader;
import static org.assertj.core.api.Assertions.assertThat;

public class KafkaCloudEventsTests {

    private static KafkaCluster kafkaCluster;
    private static File dataDir;

    @BeforeAll
    public static void before() throws IOException {
        dataDir = Testing.Files.createTestingDirectory("cluster");
        kafkaCluster = new KafkaCluster().usingDirectory(dataDir)
                .deleteDataPriorToStartup(true)
                .deleteDataUponShutdown(true)
                .withPorts(2181, 9092);
        // Start a cluster and create a topic ...
        kafkaCluster.addBrokers(1).startup();
    }

    @AfterAll
    public static void after() {
        kafkaCluster.shutdown();
        Testing.Files.delete(dataDir);
    }

    @Test
    @DisplayName("Encode / Decode a 0.3 CloudEvents object with structured encoding")
    void loopbackCloudEventWithStructuredEncoding() throws Exception {

        final String topicName = "topicA";
        KafkaConsumer consumer;
        KafkaProducer producer;

//        kafkaCluster.createTopics(topicName);

        final Properties consumerProperties = kafkaCluster.useTo().getConsumerProperties(topicName, topicName, OffsetResetStrategy.EARLIEST);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumer = new KafkaConsumer(consumerProperties);
        consumer.subscribe(Arrays.asList(topicName));

        final Properties producerProperties = kafkaCluster.useTo().getProducerProperties(topicName);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producer = new KafkaProducer<>(producerProperties);


        final CloudEvent<String> cloudEvent = new CloudEventBuilder<String>()
                .withType("My.Type")
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("/foo"))
                .withData("Hello")
                .withDatacontenttype("string")
                .build();

        KafkaCloudEvents<String> kafkaCloudEvents = new KafkaCloudEventsImpl<>(Serdes.String());
        ProducerRecord<byte[], byte[]> p = kafkaCloudEvents.encode(cloudEvent, false, topicName, -1);

        assertThat(p.topic()).isEqualTo(topicName);
        assertThat(getHeaderIfExists(p.headers(), CONTENT_TYPE)).isPresent();

        producer.send(p);

        final CountDownLatch latch = new CountDownLatch(1);
        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(1_000));
        records.forEach(rec -> {

            CloudEvent<String> actual = kafkaCloudEvents.decode(rec);

            assertThat(actual.getSpecversion()).isEqualTo(SpecVersion.V_03.toString());
            assertThat(actual.getSource()).isEqualTo(cloudEvent.getSource());
            assertThat(actual.getId()).isEqualTo(cloudEvent.getId());
            assertThat(actual.getType()).isEqualTo(cloudEvent.getType());
            assertThat(actual.getData()).isEqualTo(cloudEvent.getData());

            latch.countDown();
        });

        final boolean done = latch.await(2, TimeUnit.SECONDS);
        assertThat(done).isTrue();
    }

    @Test
    @DisplayName("Encode / Decode a 0.3 CloudEvents object with binary encoding")
    void loopbackCloudEventWithBinaryEncoding() throws Exception {

        final String topicName = "topicB";
        KafkaConsumer consumer;
        KafkaProducer producer;

//        kafkaCluster.createTopics(topicName);

        final Properties consumerProperties = kafkaCluster.useTo().getConsumerProperties(topicName, topicName, OffsetResetStrategy.EARLIEST);
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        consumer = new KafkaConsumer(consumerProperties);
        consumer.subscribe(Arrays.asList(topicName));

        final Properties producerProperties = kafkaCluster.useTo().getProducerProperties(topicName);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producer = new KafkaProducer<>(producerProperties);


        final CloudEvent<String> cloudEvent = new CloudEventBuilder<String>()
                .withType("My.Type")
                .withId(UUID.randomUUID().toString())
                .withSource(URI.create("/foo"))
                .withData("Hello")
                .withDatacontenttype("string")
                .build();

        KafkaTransportHeaders kh = KafkaTransportHeaders.getKafkaHeadersForSpec(SpecVersion.V_03);
        KafkaCloudEvents<String> kafkaCloudEvents = new KafkaCloudEventsImpl<>(Serdes.String());
        ProducerRecord<byte[], byte[]> p = kafkaCloudEvents.encode(cloudEvent, true, topicName, -1);

        assertThat(p.topic()).isEqualTo(topicName);

        // required headers
        assertThat(getRequiredHeader(p.headers(), kh.specVersionKey())).isEqualTo(SpecVersion.V_03.toString());
        assertThat(getRequiredHeader(p.headers(), kh.idKey())).isEqualTo(cloudEvent.getId());
        assertThat(getRequiredHeader(p.headers(), kh.typeKey())).isEqualTo(cloudEvent.getType());
        assertThat(getRequiredHeader(p.headers(), kh.sourceKey())).isEqualTo(cloudEvent.getSource().toString());

        producer.send(p);

        final CountDownLatch latch = new CountDownLatch(1);
        final ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(1_000));
        records.forEach(rec -> {

            CloudEvent<String> actual = kafkaCloudEvents.decode(rec);

            assertThat(actual.getSpecversion()).isEqualTo(SpecVersion.V_03.toString());
            assertThat(actual.getSource()).isEqualTo(cloudEvent.getSource());
            assertThat(actual.getId()).isEqualTo(cloudEvent.getId());
            assertThat(actual.getType()).isEqualTo(cloudEvent.getType());
            assertThat(actual.getData()).isEqualTo(cloudEvent.getData());

            latch.countDown();
        });

        final boolean done = latch.await(2, TimeUnit.SECONDS);
        assertThat(done).isTrue();
    }
}