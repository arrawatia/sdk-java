package io.cloudevents.kafka;

import io.cloudevents.SpecVersion;
import io.cloudevents.kafka.v03.KafkaCloudEventsImpl;
import io.cloudevents.v03.CloudEvent;
import io.cloudevents.v03.CloudEventBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Serdes;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.net.URI;

import static io.cloudevents.kafka.KafkaCloudEvents.CONTENT_TYPE;
import static io.cloudevents.kafka.KafkaTransportHeaders.getHeaderIfExists;
import static io.cloudevents.kafka.KafkaTransportHeaders.getRequiredHeader;
import static org.assertj.core.api.Assertions.assertThat;

public class KafkaCloudEventsTests {

    private ConsumerRecord<byte[], byte[]> toConsumerRecord(ProducerRecord<byte[], byte[]> p) {
        return new ConsumerRecord<byte[], byte[]>(p.topic(),
                -1,
                100,
                ConsumerRecord.NO_TIMESTAMP,
                TimestampType.NO_TIMESTAMP_TYPE,
                (long) ConsumerRecord.NULL_CHECKSUM,
                ConsumerRecord.NULL_SIZE,
                ConsumerRecord.NULL_SIZE,
                p.key(),
                p.value(),
                p.headers());
    }

    @Test
    @DisplayName("Encode / Decode a 0.3 CloudEvents object with structured encoding")
    void loopbackCloudEventWithStructuredEncoding() {
        // given
        final CloudEvent<String> cloudEvent = new CloudEventBuilder<String>()
                .withSource(URI.create("http://knative-eventing.com"))
                .withId("foo-bar")
                .withType("pushevent")
                .withData("{\"foo\":\"bar\"}}")
                .build();

        // encode
        KafkaCloudEvents<String> kafkaCloudEvents = new KafkaCloudEventsImpl<>(Serdes.String());
        ProducerRecord<byte[], byte[]> p = kafkaCloudEvents.encode(cloudEvent, false, "foo", -1);

        // assert
        assertThat(p.topic()).isEqualTo("foo");
        assertThat(getHeaderIfExists(p.headers(), CONTENT_TYPE)).isPresent();

        ConsumerRecord<byte[], byte[]> c = toConsumerRecord(p);
        CloudEvent<String> actual = kafkaCloudEvents.decode(c);

        assertThat(actual.getSpecversion()).isEqualTo(SpecVersion.V_03.toString());
        assertThat(actual.getSource()).isEqualTo(cloudEvent.getSource());
        assertThat(actual.getId()).isEqualTo(cloudEvent.getId());
        assertThat(actual.getType()).isEqualTo(cloudEvent.getType());
        assertThat(actual.getData()).isEqualTo(cloudEvent.getData());
    }

    @Test
    @DisplayName("Encode / Decode a 0.3 CloudEvents object with binary encoding")
    void loopbackCloudEventWithBinaryEncoding() {
        // given
        final CloudEvent<String> cloudEvent = new CloudEventBuilder<String>()
                .withSource(URI.create("http://knative-eventing.com"))
                .withId("foo-bar")
                .withType("pushevent")
                .withData("{\"foo\":\"bar\"}}")
                .build();

        // encode
        KafkaTransportHeaders kh = KafkaTransportHeaders.getKafkaHeadersForSpec(SpecVersion.V_03);
        KafkaCloudEvents<String> kafkaCloudEvents = new KafkaCloudEventsImpl<>(Serdes.String());
        ProducerRecord<byte[], byte[]> p = kafkaCloudEvents.encode(cloudEvent, true, "foo", -1);

        // assert
        assertThat(p.topic()).isEqualTo("foo");
        // required headers
        assertThat(getRequiredHeader(p.headers(), kh.specVersionKey())).isEqualTo(SpecVersion.V_03.toString());
        assertThat(getRequiredHeader(p.headers(), kh.idKey())).isEqualTo(cloudEvent.getId());
        assertThat(getRequiredHeader(p.headers(), kh.typeKey())).isEqualTo(cloudEvent.getType());
        assertThat(getRequiredHeader(p.headers(), kh.sourceKey())).isEqualTo(cloudEvent.getSource().toString());


        ConsumerRecord<byte[], byte[]> c = toConsumerRecord(p);
        CloudEvent<String> actual = kafkaCloudEvents.decode(c);

        assertThat(actual.getSpecversion()).isEqualTo(SpecVersion.V_03.toString());
        assertThat(actual.getSource()).isEqualTo(cloudEvent.getSource());
        assertThat(actual.getId()).isEqualTo(cloudEvent.getId());
        assertThat(actual.getType()).isEqualTo(cloudEvent.getType());
        assertThat(actual.getData()).isEqualTo(cloudEvent.getData());
    }

}
