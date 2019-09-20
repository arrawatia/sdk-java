package io.cloudevents.v03;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.cloudevents.Extension;
import io.cloudevents.ExtensionFormat;
import io.cloudevents.json.ZonedDateTimeDeserializer;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import java.net.URI;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author fabiojose
 * <p>
 * Implemented using immutable data structure.
 */
@JsonInclude(value = Include.NON_ABSENT)
public class CloudEvent<T> {

    // Version 0.3 of the CloudEvents spec.
    // For more details, see https://github.com/cloudevents/spec/blob/v0.3/spec.md
    // As of Feb 26, 2019, commit 17c32ea26baf7714ad027d9917d03d2fff79fc7e
    // + https://github.com/cloudevents/spec/pull/387 -> datacontentencoding
    // + https://github.com/cloudevents/spec/pull/406 -> subject

    // Ordering of fields is the same as sdk-go

    // specversion
    // Type: String
    // Constraints:
    //  REQUIRED
    //  MUST be a non-empty string
    @NotBlank
    @Pattern(regexp = "0\\.3")
    private final String specversion;

    // Type - The type of the occurrence which has happened.
    // type
    // Type: String
    // Constraints:
    //  REQUIRED
    //  MUST be a non-empty string
    //  SHOULD be prefixed with a reverse-DNS name.
    //  The prefixed domain dictates the organization which defines the semantics of this event type.
    @NotBlank
    private final String type;

    // Source - A URI describing the event producer.
    // source
    // Type: URI-reference
    // Constraints:
    //  REQUIRED
    @NotNull
    private final URI source;

    // Subject - The subject of the event in the context of the event producer
    // (identified by `source`).
    // subject
    // Type: String
    // Constraints:
    //  OPTIONAL
    //  MUST be a non-empty string
    // TODO (sumit): Figure out annotation for: if present should be non empty
    private final String subject;

    // ID of the event; must be non-empty and unique within the scope of the producer.
    // id
    // Type: String
    // Constraints:
    //  REQUIRED
    //  MUST be a non-empty string
    //  MUST be unique within the scope of the producer
    @NotBlank
    private final String id;

    // Time - A Timestamp when the event happened.
    // time
    // Type: Timestamp
    // Constraints:
    //  OPTIONAL
    //  If present, MUST adhere to the format specified in RFC 3339
    // TODO (sumit): Add validation for RFC 3339
    private final ZonedDateTime time;

    // SchemaURL - A link to the schema that the `data` attribute adheres to.
    // schemaurl
    // Type: URI
    // Constraints:
    //  OPTIONAL
    //  If present, MUST adhere to the format specified in RFC 3986
    // TODO (sumit): Add validation for RFC 3986
    private final URI schemaurl;

    // GetDataMediaType - A MIME (RFC2046) string describing the media type of `data`.
    // TODO(from sdk-go): Should an empty string assume `application/json`, `application/octet-stream`, or auto-detect the content?
    // datacontenttype
    // Type: String per RFC 2046
    // Constraints:
    //  OPTIONAL
    //  If present, MUST adhere to the format specified in RFC 2046
    // TODO (sumit): Figure out annotation for: if present should be non empty
    private final String datacontenttype;

    // DataContentEncoding describes the content encoding for the `data` attribute. Valid: nil, `Base64`.
    // datacontentencoding
    // Type: String per RFC 2045 Section 6.1
    // Constraints:
    //  The attribute MUST be set if the data attribute contains string-encoded binary data.
    //    Otherwise the attribute MUST NOT be set.
    //  If present, MUST adhere to RFC 2045 Section 6.1
    // TODO (sumit): Figure out annotation for: if present should be non empty
    private final String datacontentencoding;

    // Extensions - Additional extension metadata beyond the base spec.
    private final Map<String, Object> extensions;

    private final T data;

    CloudEvent(String specversion,
               String type,
               URI source,
               String subject,
               String id,
               ZonedDateTime time,
               URI schemaurl,
               String datacontenttype,
               String datacontentencoding,
               Set<ExtensionFormat> extensions,
               T data) {

        this.specversion = specversion;
        this.type = type;
        this.source = source;
        this.subject = subject;
        this.id = id;
        this.time = time;
        this.schemaurl = schemaurl;
        this.datacontenttype = datacontenttype;
        this.datacontentencoding = datacontentencoding;
        this.extensions = extensions
                .stream()
                .collect(Collectors
                        .toMap(ExtensionFormat::getKey,
                                ExtensionFormat::getExtension));

        this.data = data;

    }

    public String getType() {
        return type;
    }

    public String getId() {
        return id;
    }

    public String getSpecversion() {
        return specversion;
    }

    public Optional<String> getSubject() {
        return Optional.ofNullable(subject);
    }

    public URI getSource() {
        return source;
    }

    @JsonDeserialize(using = ZonedDateTimeDeserializer.class)
    public Optional<ZonedDateTime> getTime() {
        return Optional.ofNullable(time);
    }

    public Optional<URI> getSchemaurl() {
        return Optional.ofNullable(schemaurl);
    }

    public Optional<String> getDatacontenttype() {
        return Optional.ofNullable(datacontenttype);
    }

    public Optional<String> getDatacontentencoding() {
        return Optional.ofNullable(datacontentencoding);
    }

    @JsonAnyGetter
    public Map<String, Object> getExtensions() {
        return Collections.unmodifiableMap(extensions);
    }

    @JsonAnySetter
    void addExtension(String name, Object value) {
        extensions.put(name, value);
    }

    public Optional<T> getData() {
        return Optional.ofNullable(data);
    }

    // Note that extensions are ignored.
    @JsonCreator
    public static <T> CloudEvent<T> build(
            @JsonProperty("specversion") @JsonAlias( {"specversion", "specVersion", "cloudEventsVersion"}) String specversion,
            @JsonProperty("type") @JsonAlias( {"type", "eventType"}) String type,
            @JsonProperty("source") URI source,
            @JsonProperty("subject") String subject,
            @JsonProperty("id") @JsonAlias( {"id", "eventID"}) String id,
            @JsonProperty("time") @JsonAlias( {"time", "eventTime"}) ZonedDateTime time,
            @JsonProperty("schemaurl") URI schemaurl,
            @JsonProperty("datacontenttype") @JsonAlias( {"contenttype", "datacontenttype"}) String datacontenttype,
            @JsonProperty("datacontentencoding") String datacontentencoding,
            @JsonProperty("data") T data) {

        return new CloudEventBuilder<T>()
                .withId(id)
                .withSource(source)
                .withType(type)
                .withTime(time)
                .withSchemaurl(schemaurl)
                .withDatacontenttype(datacontenttype)
                .withDatacontentencoding(datacontentencoding)
                .withData(data)
                .build();
    }

    @Override
    public String toString() {
        return "CloudEvent{" +
                "specversion='" + specversion + '\'' +
                ", type='" + type + '\'' +
                ", source=" + source +
                ", subject='" + subject + '\'' +
                ", id='" + id + '\'' +
                ", time=" + time +
                ", schemaurl=" + schemaurl +
                ", datacontenttype='" + datacontenttype + '\'' +
                ", datacontentencoding='" + datacontentencoding + '\'' +
                ", extensions=" + extensions +
                ", data=" + data +
                '}';
    }
}
