package io.cloudevents.kafka;

import io.cloudevents.SpecVersion;
import io.cloudevents.kafka.v03.V03KafkaTransportHeaders;

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
}
