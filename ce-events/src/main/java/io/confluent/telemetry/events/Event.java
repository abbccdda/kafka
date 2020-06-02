/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.telemetry.events;

import io.cloudevents.CloudEvent;
import io.cloudevents.v03.AttributesImpl;
import io.cloudevents.v03.CloudEventBuilder;
import io.confluent.telemetry.events.cloudevents.extensions.RouteExtension;
import java.net.URI;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Objects;
import java.util.UUID;

// TODO(sumit): Add parity with the Go SDK. Set the right subject, source, schema based on CRN ...
public class Event {

  public static <T> Builder<T> newBuilder() {
    return new Builder<T>();
  }

  public static final class Builder<T> {

    private String type;
    private URI source;
    private String subject;
    private String id;
    private ZonedDateTime time;
    private T data;
    private String route;
    private String dataContentType;

    private Builder() {
    }

    public Builder<T> setType(String type) {
      this.type = type;
      return this;
    }

    public Builder<T> setSource(String source) {
      this.source = URI.create(source);
      return this;
    }

    public Builder<T> setSubject(String subject) {
      this.subject = subject;
      return this;
    }

    public Builder<T> setId(String id) {
      this.id = id;
      return this;
    }

    public Builder<T> setTime(ZonedDateTime time) {
      this.time = time;
      return this;
    }

    public Builder<T> setData(T data) {
      this.data = data;
      return this;
    }

    public Builder<T> setRoute(String route) {
      this.route = route;
      return this;
    }

    public Builder<T> setDataContentType(String dataContentType) {
      this.dataContentType = dataContentType;
      return this;
    }

    public CloudEvent<AttributesImpl, T> build() {
      Objects.requireNonNull(source);
      Objects.requireNonNull(subject);
      Objects.requireNonNull(type);
      Objects.requireNonNull(data);
      Objects.requireNonNull(dataContentType);

      if (this.id == null) {
        this.id = UUID.randomUUID().toString();
      }

      if (this.time == null) {
        this.time = ZonedDateTime.now(ZoneOffset.UTC);
      }

      CloudEventBuilder<T> builder = CloudEventBuilder
          .<T>builder()
          .withType(type)
          .withSource(source)
          .withId(id)
          .withTime(time)
          .withSubject(subject)
          .withData(data)
          .withDatacontenttype(dataContentType);

      if (this.route != null) {
        RouteExtension re = new RouteExtension();
        re.setRoute(this.route);
        builder.withExtension(new RouteExtension.Format(re));
      }

      return builder.build();
    }
  }
}
