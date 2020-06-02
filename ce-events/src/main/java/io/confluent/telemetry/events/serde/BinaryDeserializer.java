/*
 * Copyright [2020 - 2020] Confluent Inc.
 */
package io.confluent.telemetry.events.serde;

import io.cloudevents.extensions.DistributedTracingExtension;
import io.cloudevents.format.BinaryUnmarshaller;
import io.cloudevents.format.builder.HeadersStep;
import io.cloudevents.fun.DataUnmarshaller;
import io.cloudevents.v03.AttributesImpl;
import io.cloudevents.v03.CloudEventBuilder;
import io.cloudevents.v03.kafka.AttributeMapper;
import io.cloudevents.v03.kafka.ExtensionMapper;
import io.confluent.telemetry.events.cloudevents.extensions.RouteExtension;

public class BinaryDeserializer<T> extends Deserializer<T> {

  public BinaryDeserializer(
      String mimeType, DataUnmarshaller<byte[], T, AttributesImpl> binaryUnmarshaller) {

    this.builder = binaryBuilder(mimeType, binaryUnmarshaller);
  }

  /**
   * The default Kafka UnMarshallers do not provide hooks to customize the unmarshalling of the
   * cloudevent payload and custom extensions. See https://github.com/cloudevents/sdk-java/blob/v0.3.1/api/README.md#unmarshaller
   * for details on step builder pattern used below.
   */
  private HeadersStep<AttributesImpl, T, byte[]> binaryBuilder(String mimeType,
      DataUnmarshaller<byte[], T, AttributesImpl> binaryUnmarshaller) {
    /*
     * Step 0. Define the types - there are three
     *   - Type 1 -> AttributesImpl: the implementation of attributes
     *   - Type 2 -> T..........: the type CloudEvents' 'data'
     *   - Type 3 -> byte[]........: the type of payload used in the unmarshalling
     */

    return BinaryUnmarshaller.<AttributesImpl, T, byte[]>
        builder()
        /*
         * Step 1. Map headers Map to attributes Map
         */
        .map(AttributeMapper::map)
        /*
         * Step 2. The attributes ummarshalling - unmarshal a Map of attributes into
         *   an instance of Attributes
         */
        .map(AttributesImpl::unmarshal)
        /*
         * Step 3. The data umarshallers - unmashal the payload into  the actual 'data' type
         * instance. Supports additional unmarshaller for non-json payloads
         */
        .map(mimeType, binaryUnmarshaller)
        .next()
        /*
         * Step 4. The extension mapping - map from transport headers to map of extensions
         */
        .map(ExtensionMapper::map)
        .map(DistributedTracingExtension::unmarshall)
        .map(RouteExtension::unmarshall)
        .next()
        /*
         * Step 5. The CloudEvent builder -  take the extensions, data and attributes
         * and build CloudEvent instances. Create HeadersStep<AttributesImpl, T, byte[]>, with
         * withHeaders(), withPayload() and unmarshal() methods
         */
        .builder(CloudEventBuilder.builder());
  }


}
