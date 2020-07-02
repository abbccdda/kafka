package io.confluent.telemetry.events.exporter.http;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageLite;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.JsonFormat;
import io.cloudevents.CloudEvent;
import io.cloudevents.v1.AttributesImpl;
import io.cloudevents.v0.proto.Spec;
import io.confluent.telemetry.events.cloudevents.extensions.RouteExtension;
import io.confluent.telemetry.events.serde.Protobuf;
import io.confluent.telemetry.events.v1.EventServiceRequest;
import io.confluent.telemetry.events.v1.EventServiceResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.confluent.telemetry.events.serde.Json.createJacksonMapper;
import static io.confluent.telemetry.events.serde.Json.uncheckedEncode;

public class EventHttpExporter<T> extends
        HttpExporter<CloudEvent<AttributesImpl, T>, EventServiceRequest, EventServiceResponse> {

    private static final Logger log = LoggerFactory.getLogger(EventHttpExporter.class);

    public static final String V1_EVENTS_ENDPOINT = "/v1/events";
    private ObjectMapper mapper = createJacksonMapper();

    public EventHttpExporter() {
        this.requestConverter = events -> EventServiceRequest
                .newBuilder()
                .addAllEvents(toProto(events))
                .build();

        this.responseDeserializer = bytes -> {
            try {
                return EventServiceResponse.parseFrom(bytes);
            } catch (InvalidProtocolBufferException e) {
                throw new RuntimeException(e);
            }
        };
        this.endpoint = V1_EVENTS_ENDPOINT;
    }

    private List<Spec.CloudEvent> toProto(Collection<CloudEvent<AttributesImpl, T>> events) {
        return events.stream().map(e -> {
            Spec.CloudEvent.Builder t = Spec.CloudEvent.newBuilder()
                    .setId(e.getAttributes().getId())
                    .setSource(e.getAttributes().getSource().toString())
                    .setSpecVersion(e.getAttributes().getSpecversion())
                    .setType(e.getAttributes().getType());

            e.getAttributes().getDatacontenttype().ifPresent(a -> t.putAttributes("datacontenttype", Spec.CloudEvent.CloudEventAttribute.newBuilder().setCeString(a).build()));
            e.getAttributes().getSubject().ifPresent(a -> t.putAttributes("subject", Spec.CloudEvent.CloudEventAttribute.newBuilder().setCeString(a).build()));
            e.getAttributes().getDataschema().ifPresent(a -> t.putAttributes("dataschema", Spec.CloudEvent.CloudEventAttribute.newBuilder().setCeString(a.toString()).build()));
            e.getAttributes().getTime().ifPresent(a -> t.putAttributes("time", Spec.CloudEvent.CloudEventAttribute.newBuilder().setCeTimestamp(toTimestamp(a)).build()));
            Optional.ofNullable(e.getExtensions().get(RouteExtension.Format.IN_MEMORY_KEY)).ifPresent(a -> t.putAttributes(RouteExtension.Format.IN_MEMORY_KEY, Spec.CloudEvent.CloudEventAttribute.newBuilder().setCeString(routeToString(a)).build()));
            String dataContentType = e.getAttributes().getDatacontenttype().orElse("null");

            switch (dataContentType) {
                case Protobuf.APPLICATION_PROTOBUF:
                    if (e.getData().isPresent()) {
                        MessageLite d = (MessageLite) e.getData().get();
                        t.setBinaryData(d.toByteString());
                    }
                    break;
                // The config events have a protobuf payload but the cloudevent spec allows any object. This case
                // makes sure that we can support non-proto objects in the future.
                case Protobuf.APPLICATION_JSON:
                    if (e.getData().isPresent()) {
                        byte[] serializedPayload = uncheckedEncode(mapper, e.getData().get());
                        t.setBinaryData(ByteString.copyFrom(serializedPayload));
                    }
                    break;
                default:
                    String jsonMessage = null;
                    try {
                        jsonMessage = JsonFormat.printer().includingDefaultValueFields().print(t);
                    } catch (InvalidProtocolBufferException invalidProtocolBufferException) {
                        log.error("error serializing protobuf {}", e);
                    }
                    log.error("unsupported content type {} on event {}", dataContentType, jsonMessage);
                    break;
            }

            return t.build();

        }).collect(Collectors.toList());
    }

    private String routeToString(Object a) {
        if (a instanceof RouteExtension) {
            RouteExtension r = (RouteExtension) a;
            return r.getRoute();
        }
        return null;
    }

    private Timestamp toTimestamp(ZonedDateTime a) {
        Instant instant = Instant.from(a);
        return Timestamp.newBuilder()
                .setSeconds(instant.getEpochSecond())
                .setNanos(instant.getNano())
                .build();
    }
}
