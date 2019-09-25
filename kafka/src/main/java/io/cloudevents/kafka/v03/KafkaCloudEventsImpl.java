/**
 * Copyright 2018 The CloudEvents Authors
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.cloudevents.kafka.v03;

import com.fasterxml.jackson.core.type.TypeReference;
import io.cloudevents.SpecVersion;
import io.cloudevents.json.Json;
import io.cloudevents.kafka.KafkaCloudEvents;
import io.cloudevents.kafka.KafkaTransportHeaders;
import io.cloudevents.v03.CloudEvent;
import io.cloudevents.v03.CloudEventBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.net.URI;
import java.time.ZonedDateTime;

import static io.cloudevents.kafka.KafkaTransportHeaders.getHeaderIfExists;
import static io.cloudevents.kafka.KafkaTransportHeaders.getRequiredHeader;
import static io.cloudevents.kafka.KafkaTransportHeaders.header;
import static java.time.format.DateTimeFormatter.ISO_ZONED_DATE_TIME;

public final class KafkaCloudEventsImpl<T> implements KafkaCloudEvents<T> {


    private final Serde<T> serde;

    public KafkaCloudEventsImpl(Serde<T> serde) {
        this.serde = serde;
    }


    @Override
    public CloudEvent<T> decode(ConsumerRecord<byte[], byte[]> record) {
        final Headers headers = record.headers();
        final CloudEventBuilder<T> builder = new CloudEventBuilder<T>();

        // https://github.com/cloudevents/spec/blob/master/kafka-transport-binding.md#3-kafka-message-mapping
        // The receiver of the event can distinguish between the two content modes by inspecting the
        // ce_datacontenttype Header of the Kafka message. If the value is prefixed with the
        // CloudEvents media type application/cloudevents, indicating the use of a known event format,
        // the receiver uses structured mode, otherwise it defaults to binary mode.
        // If a receiver finds a CloudEvents media type as per the above rule, but with an event format that
        // it cannot handle, for instance application/cloudevents+avro, it MAY still treat the event as binary
        // and forward it to another party as-is.

        // https://github.com/cloudevents/spec/blob/master/kafka-transport-binding.md#33-structured-content-mode
        // The [Kafka content-type] property field MUST be set to the media type of an event format.
        // Example for the JSON format: content-type: application/cloudevents+json; charset=UTF-8

        // TL;DR - structured encoding only if ce_datacontenttype=application/cloudevents+json

        boolean structured = false;
        if (getHeaderIfExists(headers, CONTENT_TYPE).isPresent()) {
            structured = getHeaderIfExists(headers, CONTENT_TYPE).get().equalsIgnoreCase(STRUCTURED_TYPE);
        }
        ;

        if (structured) {
            return Json.decodeValue(Serdes.String().deserializer().deserialize(record.topic(), record.value()), new TypeReference<CloudEvent<T>>() {
            });
        } else {

            final KafkaTransportHeaders transportHeaders = KafkaTransportHeaders.getKafkaHeadersForSpec(SpecVersion.V_03);


            builder.withType(getRequiredHeader(headers, transportHeaders.typeKey()))
                    .withSource(URI.create(getRequiredHeader(headers, transportHeaders.sourceKey())))
                    .withId(getRequiredHeader(headers, transportHeaders.idKey()));

            getHeaderIfExists(headers, transportHeaders.timeKey()).ifPresent(k ->
                    builder.withTime(ZonedDateTime.parse(k, ISO_ZONED_DATE_TIME))
            );
            getHeaderIfExists(headers, transportHeaders.schemaUrlKey()).ifPresent(k ->
                    builder.withSchemaurl(URI.create(k))
            );
            getHeaderIfExists(headers, transportHeaders.dataContentTypeKey()).ifPresent(k ->
                    builder.withDatacontenttype(k)
            );


            //todo: add extensions
            // https://github.com/cloudevents/sdk-go/blob/master/pkg/cloudevents/transport/http/codec_v03.go#L236

//                if (extensions != null && extensions.length > 0) {
//
//                    // move this out
//                    Arrays.asList(extensions).forEach(ext -> {
//
//                        try {
//                            Object extObj  = ext.newInstance();
//                            final JsonObject extension = new JsonObject();
//                            Field[] fields = ext.getDeclaredFields();
//
//                            for (Field field : fields) {
//                                boolean accessible = field.isAccessible();
//                                field.setAccessible(true);
//                                field.set(extObj, request.headers().get(field.getName()));
//                                field.setAccessible(accessible);
//                            }
//                            builder.extension((Extension) extObj);
//                        } catch (InstantiationException e) {
//                            e.printStackTrace();
//                        } catch (IllegalAccessException e) {
//                            e.printStackTrace();
//                        }
//                    });
//                }

            builder.withData(this.serde.deserializer().deserialize(null, headers, record.value()));

            return builder.build();
        }
    }

    @Override
    public ProducerRecord<byte[], byte[]> encode(CloudEvent<T> cloudEvent, boolean binary, String topic, int partition) {

        final RecordHeaders headers = new RecordHeaders();
        byte[] key = null;
        byte[] value = null;

        //TODO(sumit): add a check that the payload in v0.3

        if (binary) {

            final KafkaTransportHeaders transportHeaders = KafkaTransportHeaders.getKafkaHeadersForSpec(SpecVersion.V_03);

            // read required headers
            // specversion
            headers.add(header(transportHeaders.specVersionKey(), SpecVersion.V_03.toString()));
            // type
            headers.add(header(transportHeaders.typeKey(), cloudEvent.getType()));
            // source
            headers.add(header(transportHeaders.sourceKey(), cloudEvent.getSource().toString()));
            // id
            headers.add(header(transportHeaders.idKey(), cloudEvent.getId()));

            // datacontenttype
            // https://github.com/cloudevents/spec/blob/master/kafka-transport-binding.md#32-binary-content-mode
            // For the binary mode, the header ce_datacontenttype property MUST be mapped directly to the CloudEvents datacontenttype attribute.

            if (cloudEvent.getDatacontenttype().isPresent()) {
                headers.add(header(transportHeaders.dataContentTypeKey(), cloudEvent.getDatacontenttype().get()));
            }
            // read optional headers
            // schema url
            if (cloudEvent.getSchemaurl().isPresent()) {
                headers.add(header(transportHeaders.schemaUrlKey(), cloudEvent.getSchemaurl().get().toString()));
            }

            // time
            if (cloudEvent.getTime().isPresent()) {
                headers.add(header(transportHeaders.timeKey(), cloudEvent.getTime().get().toString()));
            }

            // https://github.com/cloudevents/sdk-go/blob/master/pkg/cloudevents/transport/http/codec_v03.go#L128
            // Per spec, map-valued extensions are converted to a list of headers as:
            // CE-attrib-key

//            cloudEvent.getExtensions().ifPresent(extensions -> {
//                extensions.forEach(ext -> {
//                    JsonObject.mapFrom(ext).forEach(extEntry -> {
//                        request.putHeader(HttpHeaders.createOptimized(extEntry.getKey()), HttpHeaders.createOptimized(extEntry.getValue().toString()));
//                    });
//                });
//            });


            // TODO(sumit): Get the partition key as a constant.
//                if (cloudEvent.getExtensions().containsKey("ce-partitionkey")) {
//                    key = ((String) cloudEvent.getExtensions().get("ce-partitionkey")).getBytes();
//                }

            value = this.serde.serializer().serialize(topic, headers, cloudEvent.getData().get());


        } else {
            //https: github.com/cloudevents/sdk-go/blob/master/pkg/cloudevents/transport/pubsub/codec_v03.go#L53
            headers.add(header(CONTENT_TYPE, STRUCTURED_TYPE));
            value = Json.encode(cloudEvent).getBytes();
        }
        return new ProducerRecord<byte[], byte[]>(topic, null, key, value, headers);
    }


}
