/*
 * Copyright [2017  - 2017] Confluent Inc.
 */

package io.confluent.license;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Time;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jws.JsonWebSignature;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.ReservedClaimNames;
import org.jose4j.lang.JoseException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static io.confluent.license.License.baseClaims;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.newCapture;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.powermock.api.easymock.PowerMock.mockStaticPartial;

@RunWith(PowerMockRunner.class)
@PrepareForTest({License.class, LicenseStore.class, LicenseManager.class})
@PowerMockIgnore("javax.management.*")
public class LicenseManagerTest extends EasyMockSupport {

  private static final Logger log = LoggerFactory.getLogger(LicenseManager.class);
  private static final String TOPIC = "_confluent-command";
  public static final KeyPair KEY_PAIR = generateKeyPair();

  private MockTime time;
  @Mock
  private LicenseStore store;

  private LicenseManager manager;
  private License license;
  private String validRegularLicenseStr;
  private String expiredRegularLicenseStr;
  private String invalidSignedLicenseStr;
  private String invalidExpirationLicenseStr;
  private long expirationMillis;
  private LinkedList<LicenseChanged> events;
  private Consumer<LicenseChanged> listener;

  public static KeyPair generateKeyPair() {
    try {
      KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
      keyGen.initialize(2048);
      return keyGen.genKeyPair();
    } catch (NoSuchAlgorithmException e) {
      // This can happen for two reasons:
      //  1. The public key was not packaged with the jar.
      //  2. The public key which was packaged was corrupt.
      throw new IllegalStateException("Internal license validation error", e);
    }
  }

  public static String sign(PrivateKey key, long expiration) {
    try {
      return License.sign(key, null, expiration, true);
    } catch (JoseException e) {
      log.error("Error creating license for test");
    }
    return null;
  }

  public static String generateLicense(Time clock, int daysBeforeExpiring) {
    return generateLicense(clock.milliseconds() + TimeUnit.DAYS.toMillis(daysBeforeExpiring));
  }

  public static String generateLicense(long expiryTimeMs) {
    return sign(KEY_PAIR.getPrivate(), expiryTimeMs);
  }

  public static String generateLicenseSignedWithWrongKeyPair(Time clock, int daysBeforeExpiring) {
    KeyPair unknownKeyPair = generateKeyPair();
    return sign(
        unknownKeyPair.getPrivate(),
        clock.milliseconds() + TimeUnit.DAYS.toMillis(daysBeforeExpiring)
    );
  }

  public static String generateLicenseSignedWithInvalidExpiration(Time clock) {
    JwtClaims claims = baseClaims(null, -1, true);
    claims.setClaim(ReservedClaimNames.EXPIRATION_TIME, "1234");
    JsonWebSignature jws = new JsonWebSignature();
    jws.setPayload(claims.toJson());
    jws.setKey(KEY_PAIR.getPrivate());
    jws.setAlgorithmHeaderValue(AlgorithmIdentifiers.RSA_USING_SHA256);
    try {
      return jws.getCompactSerialization();
    } catch (JoseException e) {
      log.error("Error creating license for test");
    }
    return null;
  }

  public static License generateLicenseObject(
      String audience,
      Time clock,
      int expirationDays
  ) throws JoseException {
    long expirationMillis = clock.milliseconds() + TimeUnit.DAYS.toMillis(expirationDays);
    JwtClaims claims = baseClaims(audience, expirationMillis, true);
    JsonWebSignature jws = new JsonWebSignature();
    jws.setPayload(claims.toJson());
    jws.setKey(KEY_PAIR.getPrivate());
    jws.setAlgorithmHeaderValue(AlgorithmIdentifiers.RSA_USING_SHA256);
    String serialized = jws.getCompactSerialization();
    return new License(claims, clock, serialized);
  }

  @Before
  public void setUp() throws Exception {
    events = new LinkedList<>();
    listener = events::add;
    time = new MockTime();
    time.sleep(TimeUnit.DAYS.toMillis(TimeUnit.MILLISECONDS.toDays(Time.SYSTEM.milliseconds())));
    store = createMock(LicenseStore.class);
    validRegularLicenseStr = generateLicense(time, 360);
    expiredRegularLicenseStr = generateLicense(time, -5);
    invalidSignedLicenseStr = generateLicenseSignedWithWrongKeyPair(time, 360);
    invalidExpirationLicenseStr = generateLicenseSignedWithInvalidExpiration(time);
    expirationMillis = time.milliseconds() + TimeUnit.DAYS.toMillis(360);
  }

  @Test(expected = ConfigException.class)
  public void testOldConstructor() {
    Map<String, Object> empty = Collections.emptyMap();
    new LicenseManager(TOPIC, empty, empty, empty);
  }

  @Test
  public void shouldStartStop() throws Exception {
    store.start();
    expectLastCall();
    store.stop();
    expectLastCall();

    replayAll();

    manager = new LicenseManager(() -> 1, store, time);
    manager.stop();

    verifyAll();
  }

  @Test
  public void shouldAddAndRemoveClusterClients() {
    store.start();
    expectLastCall();

    replayAll();

    manager = new LicenseManager(() -> 1, store, time);

    Map<String, Object> clusterA = Collections.singletonMap("x", "clusterA");
    Map<String, Object> clusterB = Collections.singletonMap("x", "clusterB");
    Map<String, Object> clusterC = Collections.singletonMap("x", "clusterC");
    Map<String, Object> clusterD = Collections.singletonMap("x", "clusterD");
    manager.addCluster("a", Collections.singletonMap("x", "clusterA"));
    manager.addCluster("b", () -> 1);
    manager.addCluster("c", Collections.singletonMap("x", "clusterC"));
    manager.addCluster("a", () -> 2);
    assertTrue(manager.removeCluster("b"));
    assertTrue(manager.removeCluster("c"));
    assertFalse(manager.removeCluster("b"));
    assertFalse(manager.removeCluster("c"));
    assertFalse(manager.removeCluster("z"));
    manager.addCluster("b", () -> 1);
    manager.addCluster("a", Collections.singletonMap("x", "clusterAx"));
    assertTrue(manager.removeCluster("b"));
    assertFalse(manager.removeCluster("z"));
    assertFalse(manager.removeCluster(null));

    verifyAll();
  }

  @Test
  public void shouldUseFreeLicenseWithNoNewLicenseAndSingleOneNodeCluster() throws Exception {
    store.start();
    expectLastCall();
    expect(store.licenseScan()).andReturn(null);
    store.stop();
    expectLastCall();

    replayAll();

    manager = new LicenseManager(() -> 1, store, time);
    addClusterClients(manager, 1);
    assertTrue(manager.addListener(listener));
    license = manager.registerOrValidateLicense("");
    manager.stop();

    verifyAll();

    assertNotNull(license);
    assertNotNull(license.serializedForm());
    assertTrue(license.isFreeTier());
    assertTrue(license.timeRemaining(TimeUnit.DAYS) > 1000000L);

    assertUpdatedEvent(license, "License for single cluster");
    assertNoEvents();
  }

  @Test
  public void shouldUseFreeLicenseWithNoNewLicenseAndMultipleOneNodeClusters() throws Exception {
    store.start();
    expectLastCall();
    expect(store.licenseScan()).andReturn(null);
    store.stop();
    expectLastCall();

    replayAll();

    manager = new LicenseManager(() -> 1, store, time);
    addClusterClients(manager, 1, 1, 1);
    assertTrue(manager.addListener(listener));
    assertFalse(manager.addListener(listener));
    license = manager.registerOrValidateLicense("");
    manager.stop();

    verifyAll();

    assertNotNull(license);
    assertTrue(license.isFreeTier());
    assertTrue(license.timeRemaining(TimeUnit.DAYS) > 1000000L);

    // Should not expect duplicates even though we added our listener 2X
    assertUpdatedEvent(license, "License for single cluster");
    assertNoEvents();
  }

  @Test
  public void shouldStartTrialWithNoNewLicenseAndThreeNodeCluster() throws Exception {
    Capture<String> licenseStrCapture = newCapture();
    store.start();
    expectLastCall();
    expect(store.licenseScan()).andReturn(null);
    store.registerLicense(capture(licenseStrCapture));
    expectLastCall();
    store.stop();
    expectLastCall();

    store.start();
    expectLastCall();
    expect(store.licenseScan()).andAnswer(licenseStrCapture::getValue);
    store.stop();
    expectLastCall();

    replayAll();

    manager = new LicenseManager(() -> 1, store, time);
    addClusterClients(manager, 3);
    assertTrue(manager.addListener(listener));
    license = manager.registerOrValidateLicense("");
    manager.stop();
    long daysRemaining = license.timeRemaining(TimeUnit.DAYS);

    time.sleep(TimeUnit.DAYS.toMillis(10));

    manager = new LicenseManager(() -> 1, store, time);
    addClusterClients(manager, 3);
    assertTrue(manager.addListener(listener));
    License license2 = manager.registerOrValidateLicense("");
    manager.stop();
    long daysRemaining2 = license2.timeRemaining(TimeUnit.DAYS);

    verifyAll();

    assertNotNull(license);
    assertTrue(license.isTrial());
    assertEquals("trial", license.audienceString());
    assertEquals(30L, daysRemaining);

    assertNotNull(license2);
    assertTrue(license2.isTrial());
    assertEquals(licenseStrCapture.getValue(), license.serializedForm());
    assertEquals("trial", license.audienceString());
    assertEquals(20L, daysRemaining2);

    assertUpdatedEvent(license, "Trial license", "expires in 30 days");
    assertNoEvents();
  }

  @Test
  public void shouldStartTrialWithNoNewLicenseAndThreeNodeClusterAndSingleNodeCluster()
      throws Exception {
    store.start();
    expectLastCall();
    expect(store.licenseScan()).andReturn(null);
    store.registerLicense(anyString());
    expectLastCall();
    store.stop();
    expectLastCall();

    replayAll();

    manager = new LicenseManager(() -> 1, store, time);
    addClusterClients(manager, 3, 1);
    assertTrue(manager.addListener(listener));
    license = manager.registerOrValidateLicense("");
    manager.stop();

    verifyAll();

    assertNotNull(license);
    assertTrue(license.isTrial());
    assertEquals("trial", license.audienceString());
    assertEquals(30L, license.timeRemaining(TimeUnit.DAYS));

    assertUpdatedEvent(license, "Trial license", "expires in 30 days");
    assertNoEvents();
  }

  @Test
  public void shouldTransitionFromFreeLicenseToTrialLicenseToExpiredTrialToEnterpriseToRenewed()
      throws Exception {

    mockStaticPartial(License.class, "loadPublicKey");
    expect(License.loadPublicKey()).andReturn(KEY_PAIR.getPublic()).times(6);

    // Phase 1 expectations: scan to find no license, don't store (since free license)
    store = PowerMock.createMock(LicenseStore.class);
    store.start();
    EasyMock.expectLastCall();
    EasyMock.expect(store.licenseScan()).andReturn(null);
    store.stop();
    EasyMock.expectLastCall();

    // Phase 2 expectations: scan to find no license, store trial license
    Capture<String> licenseStrCapture = newCapture();
    LicenseStore store2 = PowerMock.createMock(LicenseStore.class);
    store2.start();
    EasyMock.expectLastCall();
    EasyMock.expect(store2.licenseScan()).andReturn(null);
    store2.registerLicense(capture(licenseStrCapture));
    EasyMock.expectLastCall();
    store2.stop();
    EasyMock.expectLastCall();

    // Phase 3 expectations: scan to find trial license
    LicenseStore store3 = PowerMock.createMock(LicenseStore.class);
    store3.start();
    EasyMock.expectLastCall();
    EasyMock.expect(store3.licenseScan()).andAnswer(licenseStrCapture::getValue);
    store3.stop();
    EasyMock.expectLastCall();

    // Phase 4 expectations: scan to find trial license (expired)
    LicenseStore store4 = PowerMock.createMock(LicenseStore.class);
    store4.start();
    EasyMock.expectLastCall();
    EasyMock.expect(store4.licenseScan()).andAnswer(licenseStrCapture::getValue);
    store4.stop();
    EasyMock.expectLastCall();

    // Phase 5 expectations: scan to find trial license (expired); store new valid license
    LicenseStore store5 = PowerMock.createMock(LicenseStore.class);
    store5.start();
    EasyMock.expectLastCall();
    EasyMock.expect(store5.licenseScan()).andAnswer(licenseStrCapture::getValue);
    store5.registerLicense(validRegularLicenseStr);
    EasyMock.expectLastCall();
    store5.stop();
    EasyMock.expectLastCall();

    // Phase 6 expectations: scan to find enterprise license (not expired); store new valid license
    LicenseStore store6 = PowerMock.createMock(LicenseStore.class);
    store6.start();
    EasyMock.expectLastCall();
    EasyMock.expect(store6.licenseScan()).andReturn(validRegularLicenseStr);
    store6.registerLicense(anyString());
    EasyMock.expectLastCall();
    store6.stop();
    EasyMock.expectLastCall();

    PowerMock.replayAll();

    // Phase 1: Start with just 1 broker
    log.info("START phase 1 LicenseManager");
    manager = new LicenseManager(() -> 1, store, time);
    addClusterClients(manager, 1);
    assertTrue(manager.addListener(listener));
    license = manager.registerOrValidateLicense("");
    manager.stop();

    // Phase 2: Start again with more brokers
    log.info("START phase 2 LicenseManager");
    manager = new LicenseManager(() -> 1, store2, time);
    addClusterClients(manager, 2);
    assertTrue(manager.addListener(listener));
    License license2 = manager.registerOrValidateLicense("");
    manager.stop();
    long daysRemaining2 = license2.timeRemaining(TimeUnit.DAYS);

    // Phase 3: Start again with just 1 broker but find existing trial
    log.info("Time passes: 10 days");
    time.sleep(TimeUnit.DAYS.toMillis(10));
    log.info("START phase 3 LicenseManager");
    manager = new LicenseManager(() -> 1, store3, time);
    addClusterClients(manager, 1);
    assertTrue(manager.addListener(listener));
    License license3 = manager.registerOrValidateLicense("");
    manager.stop();
    long daysRemaining3 = license3.timeRemaining(TimeUnit.DAYS);

    // Phase 4: Start again with just 1 broker but find expired trial
    log.info("Time passes: 40 days");
    time.sleep(TimeUnit.DAYS.toMillis(40));
    log.info("START phase 4 LicenseManager");
    manager = new LicenseManager(() -> 1, store4, time);
    addClusterClients(manager, 1);
    assertTrue(manager.addListener(listener));
    try {
      License license4 = manager.registerOrValidateLicense("");
      fail("Expected an invalid license exception but found " + license4);
    } catch (InvalidLicenseException e) {
      // expected
    }
    manager.stop();

    // Phase 5: Start again with just 1 broker but use valid license
    log.info("START phase 5 LicenseManager");
    manager = new LicenseManager(() -> 1, store5, time);
    addClusterClients(manager, 1);
    assertTrue(manager.addListener(listener));
    License license5 = manager.registerOrValidateLicense(validRegularLicenseStr);
    manager.stop();
    long daysRemaining5 = license5.timeRemaining(TimeUnit.DAYS);

    log.info("Time passes: 300 days");
    time.sleep(TimeUnit.DAYS.toMillis(300));
    long daysRemaining5a = license5.timeRemaining(TimeUnit.DAYS);

    // Phase 6 expectations: scan to find enterprise license (not expired); store new valid license
    log.info("START phase 6 LicenseManager");
    String anotherValidRegularLicenseStr = generateLicense(time, 400);
    manager = new LicenseManager(() -> 1, store6, time);
    addClusterClients(manager, 1);
    assertTrue(manager.addListener(listener));
    License license6 = manager.registerOrValidateLicense(anotherValidRegularLicenseStr);
    manager.stop();

    long daysRemaining6 = license6.timeRemaining(TimeUnit.DAYS);

    PowerMock.verifyAll();

    assertNotNull(license);
    assertTrue(license.isFreeTier());
    assertEquals(License.Type.FREE_TIER, license.type());
    assertNotNull(license.serializedForm());
    assertTrue(license.timeRemaining(TimeUnit.DAYS) > 1000000L);
    assertUpdatedEvent(license, "License for single cluster");

    assertNotNull(license2);
    assertTrue(license2.isTrial());
    assertEquals(License.Type.TRIAL, license2.type());
    assertNotNull(license2.serializedForm());
    assertEquals("trial", license2.audienceString());
    assertEquals(30L, daysRemaining2);
    assertUpdatedEvent(license2, "Trial license", "expires in 30 days");

    assertNotNull(license3);
    assertTrue(license3.isTrial());
    assertEquals(License.Type.TRIAL, license3.type());
    assertNotNull(license3.serializedForm());
    assertEquals("trial", license3.audienceString());
    assertEquals(20L, daysRemaining3);

    assertNotNull(license5);
    assertFalse(license5.isTrial());
    assertFalse(license5.isExpired());
    assertEquals(License.Type.ENTERPRISE, license5.type());
    assertEquals(validRegularLicenseStr, license5.serializedForm());
    assertEquals("", license5.audienceString());
    assertEquals(310L, daysRemaining5);
    assertEquals(10L, daysRemaining5a);

    time.sleep(TimeUnit.DAYS.toMillis(20));
    long daysRemaining6a = license6.timeRemaining(TimeUnit.DAYS);

    assertNotNull(license6);
    assertFalse(license6.isTrial());
    assertFalse(license6.isExpired());
    assertEquals(License.Type.ENTERPRISE, license6.type());
    assertEquals(anotherValidRegularLicenseStr, license6.serializedForm());
    assertEquals("", license6.audienceString());
    assertEquals(400L, daysRemaining6);
    assertEquals(380L, daysRemaining6a);

    assertExpiredEvent("Trial license", "expired on");
    assertUpdatedEvent(license5, "License for", "expires in 310 days");
    assertRenewedEvent(license6, "License for", "expires in 400 days");
    assertNoEvents();
  }

  @Test
  public void shouldUseStoredLicenseWithNoNewLicense() throws Exception {
    // PowerMock does not work with EasyMockSupport. Need to explicitly call EasyMock methods and
    // mocks
    mockStaticPartial(License.class, "loadPublicKey");
    store = PowerMock.createMock(LicenseStore.class);
    store.start();
    EasyMock.expectLastCall();
    EasyMock.expect(License.loadPublicKey()).andReturn(KEY_PAIR.getPublic());
    EasyMock.expect(store.licenseScan()).andReturn(validRegularLicenseStr);
    store.stop();
    EasyMock.expectLastCall();

    PowerMock.replayAll();

    time.sleep(TimeUnit.DAYS.toMillis(40));

    manager = new LicenseManager(() -> 1, store, time);
    assertTrue(manager.addListener(listener));
    license = manager.registerOrValidateLicense("");
    manager.stop();

    PowerMock.verifyAll();

    assertNotNull(license);
    assertFalse(license.isTrial());
    assertFalse(license.isExpired());
    assertEquals("", license.audienceString());
    assertEquals(320L, license.timeRemaining(TimeUnit.DAYS));
    time.sleep(TimeUnit.DAYS.toMillis(10));
    assertEquals(310L, license.timeRemaining(TimeUnit.DAYS));

    // No events, because the license was stored and there is no new license
    assertNoEvents();
  }

  @Test
  public void shouldUseStoredLicenseWhenNewLicenseHasInvalidExpiration() throws Exception {
    // PowerMock does not work with EasyMockSupport. Need to explicitly call EasyMock methods and
    // mocks
    mockStaticPartial(License.class, "loadPublicKey");
    store = PowerMock.createMock(LicenseStore.class);
    store.start();
    EasyMock.expectLastCall();
    EasyMock.expect(License.loadPublicKey()).andReturn(KEY_PAIR.getPublic());
    EasyMock.expect(store.licenseScan()).andReturn(validRegularLicenseStr);
    store.stop();
    EasyMock.expectLastCall();

    PowerMock.replayAll();

    time.sleep(TimeUnit.DAYS.toMillis(40));

    manager = new LicenseManager(() -> 1, store, time);
    assertTrue(manager.addListener(listener));
    license = manager.registerOrValidateLicense(invalidExpirationLicenseStr);
    manager.stop();

    PowerMock.verifyAll();

    assertNotNull(license);
    assertFalse(license.isTrial());
    assertFalse(license.isExpired());
    assertEquals("", license.audienceString());
    assertEquals(320L, license.timeRemaining(TimeUnit.DAYS));
    time.sleep(TimeUnit.DAYS.toMillis(10));
    assertEquals(310L, license.timeRemaining(TimeUnit.DAYS));

    // No event because the license was stored and new license was not valid
    assertNoEvents();
  }

  @Test
  public void shouldUseAlreadyStartedTrial() throws Exception {
    JwtClaims stillValidTrial = baseClaims(
        "trial",
        time.milliseconds() + TimeUnit.DAYS.toMillis(4) + 1000,
        true
    );
    String trialLicense = License.generateTrialLicense(stillValidTrial);
    store.start();
    expectLastCall();
    expect(store.licenseScan()).andReturn(trialLicense);
    store.stop();
    expectLastCall();

    replayAll();

    manager = new LicenseManager(() -> 1, store, time);
    assertTrue(manager.addListener(listener));
    license = manager.registerOrValidateLicense("");
    manager.stop();

    verifyAll();

    assertNotNull(license);
    assertTrue(license.isTrial());
    assertEquals("trial", license.audienceString());
    assertEquals(4L, license.timeRemaining(TimeUnit.DAYS));

    // No event because the license was previously stored
    assertNoEvents();
  }

  @Test
  public void shouldUseAndStoreNewValidLicenseWithNoPreviousStoredLicense() throws Exception {
    // PowerMock does not work with EasyMockSupport. Need to explicitly call EasyMock methods and
    // mocks
    mockStaticPartial(License.class, "loadPublicKey");
    store = PowerMock.createMock(LicenseStore.class);
    store.start();
    EasyMock.expectLastCall();
    EasyMock.expect(License.loadPublicKey()).andReturn(KEY_PAIR.getPublic());
    EasyMock.expect(store.licenseScan()).andReturn(null);
    store.registerLicense(anyString());
    EasyMock.expectLastCall();
    store.stop();
    EasyMock.expectLastCall();

    PowerMock.replayAll();

    manager = new LicenseManager(() -> 1, store, time);
    assertTrue(manager.addListener(listener));
    license = manager.registerOrValidateLicense(validRegularLicenseStr);
    manager.stop();

    PowerMock.verifyAll();

    assertNotNull(license);
    assertFalse(license.isTrial());
    assertFalse(license.isExpired());
    assertEquals("", license.audienceString());
    assertEquals(360L, license.timeRemaining(TimeUnit.DAYS));
    time.sleep(TimeUnit.DAYS.toMillis(10));
    assertEquals(360L - 10L, license.timeRemaining(TimeUnit.DAYS));
    assertEquals(expirationMillis, license.expirationMillis());

    assertUpdatedEvent(license, "License for", "expires in 360 days");
    assertNoEvents();
  }

  @Test
  public void shouldUseAndStoreValidNewLicenseWithInvalidSignatureStoredLicense() throws Exception {
    // PowerMock does not work with EasyMockSupport. Need to explicitly call EasyMock methods and
    // mocks
    mockStaticPartial(License.class, "loadPublicKey");
    Capture<String> newStoredLicense = Capture.newInstance();
    store = PowerMock.createMock(LicenseStore.class);
    store.start();
    EasyMock.expectLastCall();
    EasyMock.expect(License.loadPublicKey()).andReturn(KEY_PAIR.getPublic());
    EasyMock.expect(store.licenseScan()).andReturn(invalidSignedLicenseStr);
    store.registerLicense(capture(newStoredLicense));
    EasyMock.expectLastCall();

    PowerMock.replayAll();

    manager = new LicenseManager(() -> 1, store, time);
    assertTrue(manager.addListener(listener));
    license = manager.registerOrValidateLicense(validRegularLicenseStr);

    PowerMock.verifyAll();

    // Should now be using the supplied valid license
    assertEquals(validRegularLicenseStr, newStoredLicense.getValue());
    assertNotEquals(invalidSignedLicenseStr, newStoredLicense.getValue());
    assertNotNull(license);
    assertFalse(license.isTrial());
    assertFalse(license.isExpired());
    assertEquals("", license.audienceString());
    assertEquals(360L, license.timeRemaining(TimeUnit.DAYS));
    time.sleep(TimeUnit.DAYS.toMillis(10));
    assertEquals(360L - 10L, license.timeRemaining(TimeUnit.DAYS));
    assertEquals(expirationMillis, license.expirationMillis());

    assertUpdatedEvent(
        license,
        "New license replaces invalid stored license",
        "invalid signature",
        "license for ",
        "expires in"
    );
    assertNoEvents();
  }

  @Test
  public void shouldUseAndStoreValidNewLicenseWithInvalidStoredLicense() throws Exception {
    // PowerMock does not work with EasyMockSupport. Need to explicitly call EasyMock methods and
    // mocks
    final String invalidLicenseStr = "this is the invalid stored license";
    mockStaticPartial(License.class, "loadPublicKey");
    Capture<String> newStoredLicense = Capture.newInstance();
    store = PowerMock.createMock(LicenseStore.class);
    store.start();
    EasyMock.expectLastCall();
    EasyMock.expect(License.loadPublicKey()).andReturn(KEY_PAIR.getPublic());
    EasyMock.expect(store.licenseScan()).andReturn(invalidLicenseStr);
    store.registerLicense(capture(newStoredLicense));
    EasyMock.expectLastCall();

    PowerMock.replayAll();

    manager = new LicenseManager(() -> 1, store, time);
    assertTrue(manager.addListener(listener));
    license = manager.registerOrValidateLicense(validRegularLicenseStr);

    PowerMock.verifyAll();

    // Should now be using the supplied valid license
    assertEquals(validRegularLicenseStr, newStoredLicense.getValue());
    assertNotEquals(invalidLicenseStr, newStoredLicense.getValue());
    assertNotNull(license);
    assertFalse(license.isTrial());
    assertFalse(license.isExpired());
    assertEquals("", license.audienceString());
    assertEquals(360L, license.timeRemaining(TimeUnit.DAYS));
    time.sleep(TimeUnit.DAYS.toMillis(10));
    assertEquals(360L - 10L, license.timeRemaining(TimeUnit.DAYS));
    assertEquals(expirationMillis, license.expirationMillis());

    assertUpdatedEvent(
        license,
        "New license replaces invalid stored license",
        "license does not match expected form",
        "License for ",
        "expires in"
    );
    assertNoEvents();
  }

  @Test
  public void shouldUseButNotStoreNewLicenseThatMatchesPreviousStoredLicense() throws Exception {

    // PowerMock does not work with EasyMockSupport. Need to explicitly call EasyMock methods and
    // mocks
    mockStaticPartial(License.class, "loadPublicKey");
    store = PowerMock.createMock(LicenseStore.class);
    store.start();
    EasyMock.expectLastCall();
    EasyMock.expect(License.loadPublicKey()).andReturn(KEY_PAIR.getPublic());
    EasyMock.expect(store.licenseScan()).andReturn(validRegularLicenseStr);
    store.stop();
    EasyMock.expectLastCall();

    PowerMock.replayAll();

    time.sleep(TimeUnit.DAYS.toMillis(10));

    manager = new LicenseManager(() -> 1, store, time);
    assertTrue(manager.addListener(listener));
    license = manager.registerOrValidateLicense(validRegularLicenseStr);
    manager.stop();

    PowerMock.verifyAll();

    assertNotNull(license);
    assertFalse(license.isTrial());
    assertFalse(license.isExpired());
    assertEquals("", license.audienceString());
    assertEquals(350L, license.timeRemaining(TimeUnit.DAYS));
    time.sleep(TimeUnit.DAYS.toMillis(10));
    assertEquals(340L, license.timeRemaining(TimeUnit.DAYS));
    assertEquals(expirationMillis, license.expirationMillis());

    // No events because no license was stored
    assertNoEvents();
  }

  @Test
  public void shouldFailWithInvalidStoredLicenseAndNoNewLicense() throws Exception {
    // PowerMock does not work with EasyMockSupport. Need to explicitly call EasyMock methods and
    // mocks
    mockStaticPartial(License.class, "loadPublicKey");
    store = PowerMock.createMock(LicenseStore.class);
    store.start();
    expectLastCall();
    expect(License.loadPublicKey()).andReturn(KEY_PAIR.getPublic());
    expect(store.licenseScan()).andReturn(invalidSignedLicenseStr);

    PowerMock.replayAll();

    manager = new LicenseManager(() -> 1, store, time);
    assertTrue(manager.addListener(listener));
    try {
      license = manager.registerOrValidateLicense("");
      fail("expected exception");
    } catch (InvalidLicenseException e) {
      // expected
    }

    PowerMock.verifyAll();

    assertNoEvents();
  }

  @Test
  public void shouldFailWithInvalidExpirationStoredLicenseAndNoNewLicense() throws Exception {
    // PowerMock does not work with EasyMockSupport. Need to explicitly call EasyMock methods and
    // mocks
    mockStaticPartial(License.class, "loadPublicKey");
    store = PowerMock.createMock(LicenseStore.class);
    store.start();
    expectLastCall();
    expect(License.loadPublicKey()).andReturn(KEY_PAIR.getPublic());
    expect(store.licenseScan()).andReturn(invalidExpirationLicenseStr);

    PowerMock.replayAll();

    manager = new LicenseManager(() -> 1, store, time);
    assertTrue(manager.addListener(listener));
    try {
      license = manager.registerOrValidateLicense("");
      fail("expected exception");
    } catch (InvalidLicenseException e) {
      // expected
    }

    PowerMock.verifyAll();

    assertNoEvents();
  }

  @Test
  public void shouldFailWithInvalidNewLicense() throws Exception {
    store.start();
    expectLastCall();

    replayAll();

    manager = new LicenseManager(() -> 1, store, time);
    assertTrue(manager.addListener(listener));
    try {
      manager.registerOrValidateLicense("malarkey license");
      fail("expected exception");
    } catch (InvalidLicenseException e) {
      // expected
    }

    verifyAll();

    assertNoEvents(); // should fail on input before we do anything else
  }

  @Test
  public void shouldFailAfterTrialExpires() throws Exception {
    JwtClaims expiredTrial = baseClaims(
        "trial",
        time.milliseconds() - TimeUnit.DAYS.toMillis(4),
        true
    );
    String trialLicense = License.generateTrialLicense(expiredTrial);
    store.start();
    expectLastCall();
    expect(store.licenseScan()).andReturn(trialLicense);

    replayAll();

    manager = new LicenseManager(() -> 1, store, time);
    assertTrue(manager.addListener(listener));
    try {
      license = manager.registerOrValidateLicense("");
      fail("expected exception");
    } catch (InvalidLicenseException e) {
      // expected
    }

    verifyAll();

    assertEvent("Trial license", "expired");
    assertNoEvents();
  }

  @Test
  public void shouldFailWithExpiredStoredLicenseAndNoNewLicense() throws Exception {
    // PowerMock does not work with EasyMockSupport. Need to explicitly call EasyMock methods and
    // mocks
    mockStaticPartial(License.class, "loadPublicKey");
    store = PowerMock.createMock(LicenseStore.class);
    store.start();
    EasyMock.expectLastCall();
    EasyMock.expect(License.loadPublicKey()).andReturn(KEY_PAIR.getPublic());
    EasyMock.expect(store.licenseScan()).andReturn(expiredRegularLicenseStr);

    PowerMock.replayAll();

    manager = new LicenseManager(() -> 1, store, time);
    assertTrue(manager.addListener(listener));
    try {
      manager.registerOrValidateLicense(expiredRegularLicenseStr);
      fail("expected exception");
    } catch (ExpiredLicenseException e) {
      // expected
      assertNotNull(e.getLicense());
    }

    PowerMock.verifyAll();

    assertEvent("License for", "expired on");
    assertNoEvents();
  }

  @Test(expected = InvalidLicenseException.class)
  public void shouldFailWithNoStoredLicenseAndNewInvalidSignatureLicense() throws Exception {
    // PowerMock does not work with EasyMockSupport. Need to explicitly call EasyMock methods and
    // mocks
    mockStaticPartial(License.class, "loadPublicKey");
    store = PowerMock.createMock(LicenseStore.class);
    store.start();
    EasyMock.expectLastCall();
    EasyMock.expect(License.loadPublicKey()).andReturn(KEY_PAIR.getPublic());
    EasyMock.expect(store.licenseScan()).andReturn(null);
    store.registerLicense(anyString());
    EasyMock.expectLastCall();
    store.stop();
    EasyMock.expectLastCall();

    PowerMock.replayAll();

    manager = new LicenseManager(() -> 1, store, time);
    manager.registerOrValidateLicense(invalidSignedLicenseStr);
  }

  @Test(expected = InvalidLicenseException.class)
  public void shouldFailWithNoStoredLicenseAndNewInvalidExpirationLicense() throws Exception {
    // PowerMock does not work with EasyMockSupport. Need to explicitly call EasyMock methods and
    // mocks
    mockStaticPartial(License.class, "loadPublicKey");
    store = PowerMock.createMock(LicenseStore.class);
    store.start();
    EasyMock.expectLastCall();
    EasyMock.expect(License.loadPublicKey()).andReturn(KEY_PAIR.getPublic());
    EasyMock.expect(store.licenseScan()).andReturn(null);
    store.registerLicense(anyString());
    EasyMock.expectLastCall();
    store.stop();
    EasyMock.expectLastCall();

    PowerMock.replayAll();

    manager = new LicenseManager(() -> 1, store, time);
    manager.registerOrValidateLicense(invalidExpirationLicenseStr);
  }

  @Test
  public void shouldFailWithInvalidStoredLicenseAndInvalidExpirationNewLicense() throws Exception {
    // PowerMock does not work with EasyMockSupport. Need to explicitly call EasyMock methods and
    // mocks
    final String invalidLicenseStr = "this is the invalid stored license";
    mockStaticPartial(License.class, "loadPublicKey");
    store = PowerMock.createMock(LicenseStore.class);
    store.start();
    EasyMock.expectLastCall();
    EasyMock.expect(License.loadPublicKey()).andReturn(KEY_PAIR.getPublic());
    EasyMock.expect(store.licenseScan()).andReturn(invalidLicenseStr);

    PowerMock.replayAll();

    manager = new LicenseManager(() -> 1, store, time);
    assertTrue(manager.addListener(listener));
    try {
      license = manager.registerOrValidateLicense(invalidExpirationLicenseStr);
      fail("expected exception");
    } catch (InvalidLicenseException e) {
      // expected
    }

    PowerMock.verifyAll();

    assertNoEvents();
  }

  @Test
  public void shouldDetermineRenewal() throws Exception {
    License a = generateLicenseObject("ClientA", time, 360);
    License aRenewal = generateLicenseObject("ClientA", time, 720);
    License aCopy = generateLicenseObject("ClientA", time, 360);
    License d = generateLicenseObject("ClientD", time, 360);

    assertEquals(a, a);
    assertTrue(a.isEquivalentTo(a));
    assertTrue(a.isEquivalentTo(aCopy));
    assertTrue(aCopy.isEquivalentTo(a));
    assertTrue(aRenewal.isRenewalOf(a));
    assertFalse(a.isRenewalOf(aRenewal)); // backwards
    assertFalse(a.isRenewalOf(d)); // different audience
    assertFalse(d.isRenewalOf(a)); // different audience
    assertFalse(a.isRenewalOf(aCopy)); // same license is not a renewal
    assertFalse(aCopy.isRenewalOf(a)); // same license is not a renewal
  }

  protected void assertUpdatedEvent(
      License license,
      String... descContains) {
    assertEvent(license, LicenseChanged.Type.UPDATED, descContains);
  }

  protected void assertRenewedEvent(
      License license,
      String... descContains) {
    assertEvent(license, LicenseChanged.Type.RENEWAL, descContains);
  }

  protected void assertExpiredEvent(
      String... descContains) {
    assertEvent(null, LicenseChanged.Type.EXPIRED, descContains);
  }

  private void assertEvent(
      License license,
      LicenseChanged.Type expectedType,
      String... descContains) {
    assertFalse(events.isEmpty());
    LicenseChanged event = events.pop();
    if (license != null) {
      assertSame(license, event.license());
    }
    assertEquals(expectedType, event.type());
    for (String fragment : descContains) {
      assertTrue(
          event.description() + " does not contain " + fragment,
          event.description()
               .toLowerCase(Locale.getDefault())
               .contains(fragment.toLowerCase(Locale.getDefault()))
      );
    }
  }

  protected void assertEvent(String... descContains) {
    assertFalse(events.isEmpty());
    LicenseChanged event = events.pop();
    for (String fragment : descContains) {
      assertTrue(event.description()
                      .toLowerCase(Locale.getDefault())
                      .contains(fragment.toLowerCase(Locale.getDefault())));
    }
  }

  protected void assertNoEvents() {
    if (!events.isEmpty()) {
      log.error("Unexpected events: {}", events);
    }
    assertTrue(events.isEmpty());
  }

  protected static class MockTime extends org.apache.kafka.common.utils.MockTime {

    private long nowMillis = 0;

    @Override
    public long milliseconds() {
      return nowMillis;
    }

    @Override
    public long hiResClockMs() {
      return nowMillis;
    }

    @Override
    public long nanoseconds() {
      return TimeUnit.MILLISECONDS.toNanos(nowMillis);
    }

    @Override
    public void sleep(long ms) {
      nowMillis += ms;
    }
    
  }

  protected static void addClusterClients(LicenseManager manager, int...brokerCounts) {
    for (int brokerCount : brokerCounts) {
      manager.addCluster(UUID.randomUUID().toString(), () -> brokerCount);
    }
  }
}


