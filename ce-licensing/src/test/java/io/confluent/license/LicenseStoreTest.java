/*
 * Copyright [2017  - 2017] Confluent Inc.
 */

package io.confluent.license;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.util.Callback;
import org.apache.kafka.connect.util.KafkaBasedLog;
import org.apache.kafka.connect.util.TestFuture;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.confluent.command.record.Command.CommandConfigType;
import io.confluent.command.record.Command.CommandKey;
import io.confluent.command.record.Command.CommandMessage;
import io.confluent.command.record.Command.LicenseInfo;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(PowerMockRunner.class)
@PrepareForTest({LicenseStore.class, KafkaBasedLog.class})
@PowerMockIgnore("javax.management.*")
public class LicenseStoreTest {

  private static final String TOPIC = "_confluent-command";
  private static final TopicPartition LICENSE_TP = new TopicPartition(TOPIC, 0);
  private static final Map<String, Object> PRODUCER_PROPS = new HashMap<>();
  private static final Map<String, Object> CONSUMER_PROPS = new HashMap<>();
  private static final Set<TopicPartition> CONSUMER_ASSIGNMENT =
      new HashSet<>(Arrays.asList(LICENSE_TP));

  private static final Map<String, String> FIRST_SET = new HashMap<>();
  private static final Map<String, Object> TOPIC_PROPS = new HashMap<>();
  private static final Node LEADER = new Node(1, "broker1", 9092);
  private static final Node REPLICA = new Node(1, "broker2", 9093);
  private static final PartitionInfo LICENSE_TPINFO =
      new PartitionInfo(TOPIC, 0, LEADER, new Node[]{REPLICA}, new Node[]{REPLICA});

  static {
    PRODUCER_PROPS.put(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
        "broker1:9092,broker2:9093"
    );
  }

  static {
    CONSUMER_PROPS.put(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
        "broker1:9092,broker2:9093"
    );
  }

  static {
    FIRST_SET.put("key", "value");
    FIRST_SET.put(null, null);
  }

  static {
    TOPIC_PROPS.put(
        LicenseStore.REPLICATION_FACTOR_CONFIG,
        "1"
    );
    TOPIC_PROPS.put(
        AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
        "broker1:9092,broker2:9093"
    );
  }

  static {
    CONSUMER_PROPS.put(
        CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG,
        "broker1:9092,broker2:9093"
    );
  }

  private static final CommandKey KEY = CommandKey.newBuilder()
      .setConfigType(CommandConfigType.LICENSE_INFO)
      .setGuid("CONFLUENT_LICENSE")
      .build();
  private static final CommandMessage LICENSE_MSG0 = CommandMessage.newBuilder()
      .setLicenseInfo(LicenseInfo.newBuilder().setJwt("jwt 0").build())
      .build();
  private static final CommandMessage LICENSE_MSG1 = CommandMessage.newBuilder()
      .setLicenseInfo(LicenseInfo.newBuilder().setJwt("jwt 1").build())
      .build();
  private static final CommandMessage LICENSE_MSG2 = CommandMessage.newBuilder()
      .setLicenseInfo(LicenseInfo.newBuilder().setJwt("jwt 2").build())
      .build();
  private Time time = Time.SYSTEM;
  private LicenseStore store;

  private KafkaBasedLog<CommandKey, CommandMessage> licenseLog;

  @Mock
  private Runnable initializer;
  @Mock
  private KafkaProducer<CommandKey, CommandMessage> producer;
  private MockConsumer<CommandKey, CommandMessage> consumer;

  private Map<TopicPartition, List<ConsumerRecord<CommandKey, CommandMessage>>> consumedRecords =
      new HashMap<>();
  private AtomicReference<String> latestLicenseRef = new AtomicReference<>();
  private Callback<ConsumerRecord<CommandKey, CommandMessage>> consumedCallback =
      new LicenseStore.ConsumeCallback(latestLicenseRef);

  @Before
  public void setUp() throws Exception {
    @SuppressWarnings("unchecked")
    KafkaBasedLog<CommandKey, CommandMessage> log = PowerMock.createPartialMock(
        KafkaBasedLog.class,
        new String[]{"createConsumer", "createProducer"},
        TOPIC,
        PRODUCER_PROPS,
        CONSUMER_PROPS,
        consumedCallback,
        time,
        initializer
    );
    licenseLog = log;
    consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    consumer.updatePartitions(TOPIC, Arrays.asList(LICENSE_TPINFO));
    Map<TopicPartition, Long> beginningOffsets = new HashMap<>();
    beginningOffsets.put(LICENSE_TP, 0L);
    consumer.updateBeginningOffsets(beginningOffsets);
  }

  @Test
  public void testInitialization() throws Exception {
    expectCreateKafkaBasedLog(TOPIC, time);
    PowerMock.replayAll();

    store.setupAndCreateKafkaBasedLog(TOPIC, PRODUCER_PROPS, CONSUMER_PROPS, TOPIC_PROPS, latestLicenseRef, time);
    PowerMock.verifyAll();
  }

  @Test
  public void testStartStop() throws Exception {
    expectCreateKafkaBasedLog(TOPIC, time);

    expectStart();
    expectStop();

    PowerMock.replayAll();
    Map<TopicPartition, Long> endOffsets = new HashMap<>();
    endOffsets.put(LICENSE_TP, 0L);
    consumer.updateEndOffsets(endOffsets);
    store.setupAndCreateKafkaBasedLog(TOPIC, PRODUCER_PROPS, CONSUMER_PROPS, TOPIC_PROPS, latestLicenseRef, time);
    store.start();
    assertEquals(CONSUMER_ASSIGNMENT, consumer.assignment());

    store.stop();

    assertFalse(Whitebox.<Thread>getInternalState(licenseLog, "thread").isAlive());
    assertTrue(consumer.closed());
    PowerMock.verifyAll();
  }

  @Test
  public void testScanForLicense() throws Exception {
    expectCreateKafkaBasedLog(TOPIC, time);
    expectStart();
    // Producer flushes when read to log end is called
    producer.flush();
    PowerMock.expectLastCall();
    expectStop();

    PowerMock.replayAll();

    Map<TopicPartition, Long> endOffsets = new HashMap<>();
    endOffsets.put(LICENSE_TP, 0L);
    consumer.updateEndOffsets(endOffsets);
    store.setupAndCreateKafkaBasedLog(TOPIC, PRODUCER_PROPS, CONSUMER_PROPS, TOPIC_PROPS, latestLicenseRef, time);
    store.start();
    assertEquals(CONSUMER_ASSIGNMENT, consumer.assignment());
    assertEquals(0L, consumer.position(LICENSE_TP));

    final CountDownLatch finishedLatch = new CountDownLatch(1);
    consumer.schedulePollTask(new Runnable() {
      @Override
      public void run() {
        consumer.scheduleNopPollTask();
        consumer.scheduleNopPollTask();
        consumer.schedulePollTask(new Runnable() {
          @Override
          public void run() {
            consumer.addRecord(new ConsumerRecord<>(
                TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, KEY, LICENSE_MSG0)
            );
            consumer.addRecord(new ConsumerRecord<>(
                TOPIC, 0, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, KEY, LICENSE_MSG1)
            );
          }
        });
        consumer.scheduleNopPollTask();
        consumer.scheduleNopPollTask();
        consumer.schedulePollTask(new Runnable() {
          @Override
          public void run() {
            consumer.addRecord(new ConsumerRecord<>(
                TOPIC, 0, 2, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, KEY, LICENSE_MSG2)
            );
          }
        });
        consumer.schedulePollTask(new Runnable() {
          @Override
          public void run() {
            finishedLatch.countDown();
          }
        });
      }
    });

    assertTrue(finishedLatch.await(10000, TimeUnit.MILLISECONDS));
    String license = store.licenseScan();
    store.stop();
    assertEquals(LICENSE_MSG2.getLicenseInfo().getJwt(), license);
    assertFalse(Whitebox.<Thread>getInternalState(licenseLog, "thread").isAlive());
    assertTrue(consumer.closed());
    PowerMock.verifyAll();
  }

  @Test
  public void testRegisterLicenseAndScanForLicense() throws Exception {
    expectCreateKafkaBasedLog(TOPIC, time);
    expectStart();
    TestFuture<RecordMetadata> newLicenseFuture = new TestFuture<>();
    ProducerRecord<CommandKey, CommandMessage> newLicense = new ProducerRecord<>(
        TOPIC,
        KEY,
        LICENSE_MSG0
    );
    Capture<org.apache.kafka.clients.producer.Callback> callbackCapture = EasyMock.newCapture();
    EasyMock.expect(producer.send(EasyMock.eq(newLicense), EasyMock.capture(callbackCapture)))
        .andReturn(newLicenseFuture);

    // Producer flushes when read to log end is called
    producer.flush();
    PowerMock.expectLastCall();
    expectStop();

    PowerMock.replayAll();

    Map<TopicPartition, Long> endOffsets = new HashMap<>();
    endOffsets.put(LICENSE_TP, 0L);
    consumer.updateEndOffsets(endOffsets);
    store.setupAndCreateKafkaBasedLog(TOPIC, PRODUCER_PROPS, CONSUMER_PROPS, TOPIC_PROPS, latestLicenseRef, time);
    store.start();
    assertEquals(CONSUMER_ASSIGNMENT, consumer.assignment());
    assertEquals(0L, consumer.position(LICENSE_TP));

    // Set some keys
    final AtomicInteger invoked = new AtomicInteger(0);
    org.apache.kafka.clients.producer.Callback producerCallback = new org.apache.kafka.clients
        .producer.Callback() {
      @Override
      public void onCompletion(RecordMetadata metadata, Exception exception) {
        invoked.incrementAndGet();
      }
    };

    store.registerLicense(LICENSE_MSG0.getLicenseInfo().getJwt(), producerCallback);
    assertEquals(0, invoked.get());
    newLicenseFuture.resolve((RecordMetadata) null);
    callbackCapture.getValue().onCompletion(null, null);
    assertEquals(1, invoked.get());

    final CountDownLatch finishedLatch = new CountDownLatch(1);
    consumer.schedulePollTask(new Runnable() {
      @Override
      public void run() {
        consumer.scheduleNopPollTask();
        consumer.scheduleNopPollTask();
        consumer.schedulePollTask(new Runnable() {
          @Override
          public void run() {
            consumer.addRecord(new ConsumerRecord<>(
                TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, KEY, LICENSE_MSG0)
            );
          }
        });
        consumer.scheduleNopPollTask();
        consumer.schedulePollTask(new Runnable() {
          @Override
          public void run() {
            finishedLatch.countDown();
          }
        });
      }
    });

    assertTrue(finishedLatch.await(10000, TimeUnit.MILLISECONDS));
    String license = store.licenseScan();
    store.stop();
    assertEquals(LICENSE_MSG0.getLicenseInfo().getJwt(), license);
    assertFalse(Whitebox.<Thread>getInternalState(licenseLog, "thread").isAlive());
    assertTrue(consumer.closed());
    PowerMock.verifyAll();
  }

  @Test
  public void testScanForLicenseWithConsumerError() throws Exception {
    expectCreateKafkaBasedLog(TOPIC, time);
    expectStart();
    // Producer flushes when read to log end is called
    producer.flush();
    PowerMock.expectLastCall();
    expectStop();

    PowerMock.replayAll();

    Map<TopicPartition, Long> endOffsets = new HashMap<>();
    endOffsets.put(LICENSE_TP, 0L);
    consumer.updateEndOffsets(endOffsets);
    store.setupAndCreateKafkaBasedLog(TOPIC, PRODUCER_PROPS, CONSUMER_PROPS, TOPIC_PROPS, latestLicenseRef, time);
    store.start();
    assertEquals(CONSUMER_ASSIGNMENT, consumer.assignment());
    assertEquals(0L, consumer.position(LICENSE_TP));

    final CountDownLatch finishedLatch = new CountDownLatch(1);
    consumer.schedulePollTask(new Runnable() {
      @Override
      public void run() {
        consumer.scheduleNopPollTask();
        consumer.scheduleNopPollTask();
        consumer.schedulePollTask(new Runnable() {
          @Override
          public void run() {
            consumer.addRecord(new ConsumerRecord<>(
                TOPIC, 0, 0, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, KEY, LICENSE_MSG0)
            );
            consumer.addRecord(new ConsumerRecord<>(
                TOPIC, 0, 1, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, KEY, LICENSE_MSG1)
            );
          }
        });
        consumer.scheduleNopPollTask();
        // Trigger exception
        consumer.schedulePollTask(new Runnable() {
          @Override
          public void run() {
            consumer.setPollException(Errors.COORDINATOR_NOT_AVAILABLE.exception());
          }
        });
        consumer.scheduleNopPollTask();
        consumer.schedulePollTask(new Runnable() {
          @Override
          public void run() {
            consumer.addRecord(new ConsumerRecord<>(
                TOPIC, 0, 2, 0L, TimestampType.CREATE_TIME, 0L, 0, 0, KEY, LICENSE_MSG2)
            );
          }
        });
        consumer.schedulePollTask(new Runnable() {
          @Override
          public void run() {
            finishedLatch.countDown();
          }
        });
      }
    });

    assertTrue(finishedLatch.await(10000, TimeUnit.MILLISECONDS));
    String license = store.licenseScan();
    store.stop();
    assertEquals(LICENSE_MSG2.getLicenseInfo().getJwt(), license);
    assertFalse(Whitebox.<Thread>getInternalState(licenseLog, "thread").isAlive());
    assertTrue(consumer.closed());
    PowerMock.verifyAll();
  }

  private void expectCreateKafkaBasedLog(String topic, Time time) throws Exception {
    store = PowerMock.createPartialMock(
        LicenseStore.class,
        new String[]{"createKafkaBasedLog"},
        topic,
        latestLicenseRef,
        licenseLog,
        time
    );

    Capture<String> capturedTopic = EasyMock.newCapture();
    Capture<Map<String, Object>> capturedProducerProps = EasyMock.newCapture();
    Capture<Map<String, Object>> capturedConsumerProps = EasyMock.newCapture();
    Capture<Map<String, Object>> capturedTopicProps = EasyMock.newCapture();
    Capture<NewTopic> capturedNewTopic = EasyMock.newCapture();
    Capture<Callback<ConsumerRecord<String, byte[]>>> capturedConsumedCallback = EasyMock
        .newCapture();
    Capture<Time> capturedTime = EasyMock.newCapture();

    PowerMock.expectPrivate(
        store,
        "createKafkaBasedLog",
        EasyMock.capture(capturedTopic),
        EasyMock.capture(capturedProducerProps),
        EasyMock.capture(capturedConsumerProps),
        EasyMock.capture(capturedConsumedCallback),
        EasyMock.capture(capturedNewTopic),
        EasyMock.capture(capturedTopicProps),
        EasyMock.capture(capturedTime)
    ).andReturn(licenseLog);
  }

  private void expectStart() throws Exception {
    initializer.run();
    EasyMock.expectLastCall().times(1);

    PowerMock.expectPrivate(licenseLog, "createProducer").andReturn(producer);
    PowerMock.expectPrivate(licenseLog, "createConsumer").andReturn(consumer);
  }

  private void expectStop() {
    producer.close();
    PowerMock.expectLastCall();
    // MockConsumer close is checked after test.
  }
}
