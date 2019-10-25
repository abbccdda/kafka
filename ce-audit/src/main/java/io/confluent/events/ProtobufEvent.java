/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.events;

import static io.confluent.events.EventLoggerConfig.CLOUD_EVENT_BINARY_ENCODING;
import static io.confluent.events.EventLoggerConfig.CLOUD_EVENT_STRUCTURED_ENCODING;

import com.google.common.base.Verify;
import com.google.protobuf.Message;
import io.cloudevents.CloudEvent;
import io.cloudevents.v03.CloudEventBuilder;
import io.confluent.events.cloudevents.extensions.RouteExtension;
import java.net.URI;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.UUID;


// TODO(sumit): Add parity with the Go SDK. Set the right subject, source, schema based on CRN ...
// TODO(sumit): Use generics.
public class ProtobufEvent {

  public static final String APPLICATION_PROTOBUF = "application/protobuf";
  public static final String APPLICATION_JSON = "application/json";

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {

    private String type;
    private URI source;
    private String subject;
    private String id;
    private ZonedDateTime time;
    private Message data;
    private String route;
    private String encoding;

    private Builder() {
    }

    public Builder setType(String type) {
      this.type = type;
      return this;
    }

    public Builder setSource(String source) {
      this.source = URI.create(source);
      return this;
    }

    public Builder setSubject(String subject) {
      this.subject = subject;
      return this;
    }

    public Builder setId(String id) {
      this.id = id;
      return this;
    }

    public Builder setTime(ZonedDateTime time) {
      this.time = time;
      return this;
    }

    public Builder setData(Message data) {
      this.data = data;
      return this;
    }

    public Builder setRoute(String route) {
      this.route = route;
      return this;
    }

    public Builder setEncoding(String encoding) {
      this.encoding = encoding;
      return this;
    }

    public CloudEvent<?, ? extends Message> build() {
      Verify.verify(source != null, "source is required");
      Verify.verify(subject != null, "subject is required");
      Verify.verify(type != null, "type is required");
      Verify.verify(data != null, "data is required");
      Verify.verify(encoding != null, "encoding is required");

      if (this.id == null) {
        this.id = UUID.randomUUID().toString();
      }

      if (this.time == null) {
        this.time = ZonedDateTime.now(ZoneOffset.UTC);
      }

      CloudEventBuilder<Message> builder = CloudEventBuilder
          .<Message>builder()
          .withType(type)
          .withSource(source)
          .withId(id)
          .withTime(time)
          .withSubject(subject)
          .withData(data);

      // This has to be handled here instead of the marshalling stage because the cloudevent object is
      // immutable once it is created. Doing it at the marshalling stage can be done by reflection or
      // creating another object with the correct data encoding. Both of these approaches are not optimal
      // Also, cloud event encoding is set at startup and does not change at runtime. So, it is fine to do
      // this here.
      if (this.encoding.equals(CLOUD_EVENT_BINARY_ENCODING)) {
        builder.withDatacontenttype(APPLICATION_PROTOBUF);
      } else if (this.encoding.equals(CLOUD_EVENT_STRUCTURED_ENCODING)) {
        builder.withDatacontenttype(APPLICATION_JSON);
      } else {
        throw new RuntimeException("Invalid cloudevent encoding: " + encoding);
      }

      if (this.route != null) {
        RouteExtension re = new RouteExtension();
        re.setRoute(this.route);
        builder.withExtension(new RouteExtension.Format(re));
      }

      return builder.build();
    }
  }
}
