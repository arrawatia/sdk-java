///**
// * Copyright 2018 The CloudEvents Authors
// * <p>
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// * <p>
// * http://www.apache.org/licenses/LICENSE-2.0
// * <p>
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package io.cloudevents.kafka.v03;
//
//import io.cloudevents.v03.CloudEvent;
//import io.cloudevents.v03.CloudEventBuilder;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.producer.ProducerRecord;
//import org.apache.kafka.common.header.Header;
//import org.apache.kafka.common.header.Headers;
//import org.apache.kafka.common.header.internals.RecordHeader;
//import org.apache.kafka.common.header.internals.RecordHeaders;
//import org.apache.kafka.common.serialization.Serde;
//import org.apache.kafka.common.serialization.Serdes;
//
//import java.net.URI;
//import java.time.ZonedDateTime;
//import java.util.Optional;
//
//import static java.time.format.DateTimeFormatter.ISO_ZONED_DATE_TIME;
//
//public final class BinaryCodec<T> {
//
//    private static final KafkaTransportHeaders transportHeaders = new KafkaTransportHeaders();
//
//    private Serde<T> serde;
//
//    /**
//     * @param serde
//     */
//    public BinaryCodec(Serde<T> serde) {
//        this.serde = serde;
//    }
//
//    /**
//     * @param name
//     * @param value
//     * @return
//     */
//    private Header header(String name, String value) {
//        byte[] valueAsBytes = ((Serde) Serdes.serdeFrom(String.class)).serializer().serialize(null, value);
//        return new RecordHeader(name, valueAsBytes);
//    }
//
//    /**
//     * @param cloudEvent
//     * @return
//     */
//    private Headers headersFrom(final CloudEvent<T> cloudEvent) {
//        final RecordHeaders headers = new RecordHeaders();
//
//        // specversion
//        headers.add(header(transportHeaders.specVersionKey(), cloudEvent.getSpecversion()));
//        // type
//        headers.add(header(transportHeaders.typeKey(), cloudEvent.getType()));
//        // source
//        headers.add(header(transportHeaders.sourceKey(), cloudEvent.getSource().toString()));
//        // id
//        headers.add(header(transportHeaders.idKey(), cloudEvent.getId()));
//
//        // datacontenttype
//        if (cloudEvent.getDatacontenttype().isPresent()) {
//            headers.add(header(transportHeaders.dataContentTypeKey(), cloudEvent.getDatacontenttype().get()));
//        }
//        // schema url
//        if (cloudEvent.getSchemaurl().isPresent()) {
//            headers.add(header(transportHeaders.schemaUrlKey(), cloudEvent.getSchemaurl().get().toString()));
//        }
//
//        // time
//        if (cloudEvent.getTime().isPresent()) {
//            headers.add(header(transportHeaders.timeKey(), cloudEvent.getTime().get().toString()));
//        }
//        // extensions
//        //todo: extensions... they are optional...
//
//        return headers;
//    }
//
//    /**
//     * @param cloudEvent
//     * @param topic
//     * @return
//     */
//    public ProducerRecord<byte[], byte[]> encode(CloudEvent<T> cloudEvent, String topic) {
//        // TODO(sumit): Get the partition key as a constant.
//        byte[] key = null;
//        if (cloudEvent.getExtensions().containsKey("ce-partitionkey")) {
//            key = ((String) cloudEvent.getExtensions().get("ce-partitionkey")).getBytes();
//        }
//        Headers headers = headersFrom(cloudEvent);
//        byte[] value = this.serde.serializer().serialize(topic, headers, cloudEvent.getData().get());
//
//        return new ProducerRecord<byte[], byte[]>(topic, null, key, value, headers);
//    }
//
//
//    private Optional<String> getHeaderIfExists(Headers headers, String headerKey) {
//        if (headers.lastHeader(headerKey) == null) {
//            return Optional.empty();
//        }
//        return Optional.of(Serdes.serdeFrom(String.class).deserializer().deserialize(null, headers.lastHeader(headerKey).value()));
//    }
//
//
//    public CloudEvent<T> decode(final ConsumerRecord<byte[], byte[]> record) {
//
//        final Headers headers = record.headers();
//        final CloudEventBuilder<T> builder = new CloudEventBuilder<T>();
//
//        try {
//            builder.withType(getHeaderIfExists(headers, transportHeaders.typeKey()).get())
//                    .withSource(URI.create(getHeaderIfExists(headers, transportHeaders.sourceKey()).get()))
//                    .withId(getHeaderIfExists(headers, transportHeaders.idKey()).get());
//
//            getHeaderIfExists(headers, transportHeaders.timeKey()).ifPresent(k -> builder.withTime(ZonedDateTime.parse(k, ISO_ZONED_DATE_TIME)));
//            getHeaderIfExists(headers, transportHeaders.schemaUrlKey()).ifPresent(k -> builder.withSchemaurl(URI.create(k)));
//            getHeaderIfExists(headers, transportHeaders.dataContentTypeKey()).ifPresent(k -> builder.withDatacontenttype(k));
//
//
//            //todo: add extensions
//
//            builder.withData(this.serde.deserializer().deserialize(null, headers, record.value()));
//
//
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//
//        return builder.build();
//    }
//}
