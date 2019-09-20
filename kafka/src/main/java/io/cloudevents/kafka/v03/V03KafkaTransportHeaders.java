/**
 * Copyright 2018 The CloudEvents Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.cloudevents.kafka.v03;

import io.cloudevents.kafka.KafkaTransportHeaders;

public class V03KafkaTransportHeaders implements KafkaTransportHeaders {
    // https://github.com/cloudevents/spec/blob/master/kafka-transport-binding.md#32-binary-content-mode
    // CloudEvent attributes are prefixed with "ce_" for use in the message-headers section.

    @Override
    public String specVersionKey() {
        return SPEC_VERSION_KEY;
    }

    @Override
    public String typeKey() {
        return PREFIX + "type";
    }

    @Override
    public String sourceKey() {
        return PREFIX + "source";
    }

    @Override
    public String idKey() {
        return PREFIX + "id";
    }

    @Override
    public String timeKey() {
        return PREFIX + "time";
    }

    @Override
    public String schemaUrlKey() {
        return PREFIX + "schemaurl";
    }

    @Override
    public String dataContentTypeKey() {
        return PREFIX + "datacontenttype";
    }

    @Override
    public String subjectKey() {
        return PREFIX + "subject";
    }

    @Override
    public String dataContentEncodingKey() {
        return PREFIX + "datacontentencoding";
    }
}
