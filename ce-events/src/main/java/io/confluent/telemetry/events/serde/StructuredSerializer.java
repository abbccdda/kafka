/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.telemetry.events.serde;

import io.cloudevents.format.StructuredMarshaller;
import io.cloudevents.format.builder.EventStep;
import io.cloudevents.fun.EnvelopeMarshaller;
import io.cloudevents.v1.AttributesImpl;

public class StructuredSerializer<T> extends Serializer<T> {

  public StructuredSerializer(EnvelopeMarshaller<AttributesImpl, T, byte[]> dataMarshaller) {
    this.builder = structuredBuilder(dataMarshaller);
  }

  /**
   * The default Kafka Marshallers do not provide hooks to customize the marshalling the cloudevent
   * payload and custom extensions.
   * See https://github.com/cloudevents/sdk-java/blob/v0.3.1/api/README.md#marshaller for details
   * on step builder pattern used below.
   */
  protected EventStep<AttributesImpl, T, byte[], byte[]> structuredBuilder(
      EnvelopeMarshaller<AttributesImpl, T, byte[]> dataMarshaller) {
    /*
     * Step 0. Define the types - there are four
     *   - Type 1 -> AttributesImpl: the implementation of attributes
     *   - Type 2 -> T..........: the type CloudEvents' 'data'
     *   - Type 3 -> byte[]........: the type of payload that will result of marshalling
     *   - Type 4 -> byte[]........: the type of headers values. String for HTTP, byte[] for Kafka.
     */
    return StructuredMarshaller.<AttributesImpl, T, byte[], byte[]>
        builder()
        /*
         * Step 1. Setting the media type for the envelope
         */
        .mime("content-type", "application/cloudevents+json".getBytes())
        /*
         * Step 2. The marshaller for envelope which marshals the cloudevents' envelope
         */
        .map(dataMarshaller)
        /*
         * Skip other steps. Based on the Default Kafka marshaller.
         */
        .skip();
  }

}
