/*
 Copyright 2018 Confluent Inc.
 */

package kafka.tier.client;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.function.Supplier;

public class MockProducerSupplier<K, V> implements Supplier<Producer<K, V>> {
    private final MockProducer<K, V> producer;

    public MockProducerSupplier() {
        this.producer = new MockProducer<>();
    }

    @Override
    public Producer<K, V> get() {
        return producer;
    }

    public MockProducer<K, V> producer() {
        return producer;
    }
}
