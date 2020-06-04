/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.telemetry.events.serde;

import io.cloudevents.extensions.ExtensionFormat;
import io.cloudevents.format.BinaryMarshaller;
import io.cloudevents.format.Wire;
import io.cloudevents.format.builder.EventStep;
import io.cloudevents.fun.DataMarshaller;
import io.cloudevents.v1.Accessor;
import io.cloudevents.v1.AttributesImpl;
import io.cloudevents.v1.kafka.HeaderMapper;

public class BinarySerializer<T> extends Serializer<T> {

  public BinarySerializer(DataMarshaller<byte[], T, byte[]> binarydataMarshaller) {
    super();
    this.builder = binaryBuilder(binarydataMarshaller);
  }

  /**
   * The default Kafka Marshallers do not provide hooks to customize the marshalling the cloudevent
   * payload and custom extensions. See https://github.com/cloudevents/sdk-java/blob/v0.3.1/api/README.md#marshaller
   * for details on step builder pattern used below.
   */
  private EventStep<AttributesImpl, T, byte[], byte[]> binaryBuilder(
      DataMarshaller<byte[], T, byte[]> binarydataMarshaller) {
    /*
     * Step 0. Define the types - there are four
     *   - Type 1 -> AttributesImpl: the implementation of attributes
     *   - Type 2 -> T..........: the type CloudEvents' 'data'
     *   - Type 3 -> byte[]........: the type of payload that will result of marshalling
     *   - Type 4 -> byte[]........: the type of headers values. String for HTTP, byte[] for Kafka . . .
     */
    return BinaryMarshaller.<AttributesImpl, T, byte[], byte[]>
        builder()
        /*
         * Step 1. Attributes marshalling - Marshal AttributesImpl into a Map<String, String>
         */

        .map(AttributesImpl::marshal)
        /*
         * Step 2. Access the internal list of extensions
         */
        .map(Accessor::extensionsOf)
        /*
         * Step 3. Extensions marshalling - marshal a Collection<ExtensionFormat> into a
         *   Map<String, String>
         */
        .map(ExtensionFormat::marshal)
        /*
         * Step 4. Mapping to Kafka headers -  map attributes and extensions into Kafka headers
         */
        .map(HeaderMapper::map)
        /*
         * Step 5. Data marshalleing - marshal the CloudEvents' data into payload for transport
         */
        .map(binarydataMarshaller)
        /*
         * Step 6. The wire builder - Makes it easy to get the marshalled payload/headers. Create
         * the EventStep<AttributesImpl, T, []byte, []byte > which has withEvent() and marshal()
         * methods
         */
        .builder(Wire::new);
  }

}
