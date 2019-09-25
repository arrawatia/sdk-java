package io.cloudevents.kafka;

import io.cloudevents.SpecVersion;
import io.cloudevents.kafka.v03.V03KafkaTransportHeaders;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Optional;

public interface KafkaTransportHeaders {
    String PREFIX = "ce-";

    String SPEC_VERSION_KEY = PREFIX + "specversion";

    String specVersionKey();

    String typeKey();

    String sourceKey();

    String idKey();

    String timeKey();

    String schemaUrlKey();

    String dataContentTypeKey();

    String subjectKey();

    String dataContentEncodingKey();

    static KafkaTransportHeaders getKafkaHeadersForSpec(final SpecVersion specVersion) {

        switch (specVersion) {

            case V_01:
                throw new RuntimeException("spec v0.1 is not supported by kafka transport");
            case V_02:
                throw new RuntimeException("spec v0.2 is not supported by kafka transport");
            case V_03:
            case DEFAULT:
                return new V03KafkaTransportHeaders();
        }

        // you should not be here!
        throw new IllegalArgumentException("Could not find proper version");
    }

    static Header header(String name, String value) {
        byte[] valueAsBytes = ((Serde) Serdes.serdeFrom(String.class)).serializer().serialize(null, value);
        return new RecordHeader(name, valueAsBytes);
    }

    static String getRequiredHeader(final Headers headers, final String headerName) {
        return getHeaderIfExists(headers, headerName).orElseThrow(IllegalArgumentException::new);
    }

    static Optional<String> getHeaderIfExists(Headers headers, String headerKey) {
        if (headers.lastHeader(headerKey) == null) {
            return Optional.empty();
        }
        return Optional.of(Serdes.serdeFrom(String.class).deserializer().deserialize(null, headers.lastHeader(headerKey).value()));
    }
}
