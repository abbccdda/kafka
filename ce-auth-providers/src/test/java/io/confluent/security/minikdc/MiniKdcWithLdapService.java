/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.security.minikdc;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.naming.Context;
import javax.naming.InvalidNameException;

import org.apache.commons.lang.text.StrSubstitutor;
import org.apache.directory.api.ldap.model.entry.Attribute;
import org.apache.directory.api.ldap.model.entry.DefaultAttribute;
import org.apache.directory.api.ldap.model.entry.DefaultEntry;
import org.apache.directory.api.ldap.model.entry.DefaultModification;
import org.apache.directory.api.ldap.model.entry.Entry;
import org.apache.directory.api.ldap.model.entry.ModificationOperation;
import org.apache.directory.api.ldap.model.exception.LdapException;
import org.apache.directory.api.ldap.model.ldif.LdifEntry;
import org.apache.directory.api.ldap.model.ldif.LdifReader;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.api.ldap.model.name.Rdn;
import org.apache.directory.api.ldap.model.schema.AttributeType;
import org.apache.directory.api.ldap.schema.extractor.impl.DefaultSchemaLdifExtractor;
import org.apache.directory.api.ldap.schema.loader.LdifSchemaLoader;
import org.apache.directory.api.ldap.schema.manager.impl.DefaultSchemaManager;
import org.apache.directory.server.constants.ServerDNConstants;
import org.apache.directory.server.core.DefaultDirectoryService;
import org.apache.directory.server.core.api.CacheService;
import org.apache.directory.server.core.api.CoreSession;
import org.apache.directory.server.core.api.DirectoryService;
import org.apache.directory.server.core.api.InstanceLayout;
import org.apache.directory.server.core.api.schema.SchemaPartition;
import org.apache.directory.server.core.kerberos.KeyDerivationInterceptor;
import org.apache.directory.server.core.partition.impl.btree.jdbm.JdbmIndex;
import org.apache.directory.server.core.partition.impl.btree.jdbm.JdbmPartition;
import org.apache.directory.server.core.partition.ldif.LdifPartition;
import org.apache.directory.server.kerberos.KerberosConfig;
import org.apache.directory.server.kerberos.kdc.KdcServer;
import org.apache.directory.server.kerberos.shared.crypto.encryption.KerberosKeyFactory;
import org.apache.directory.server.kerberos.shared.keytab.Keytab;
import org.apache.directory.server.kerberos.shared.keytab.KeytabEntry;
import org.apache.directory.server.ldap.LdapServer;
import org.apache.directory.server.ldap.handlers.sasl.gssapi.GssapiMechanismHandler;
import org.apache.directory.server.protocol.shared.transport.AbstractTransport;
import org.apache.directory.server.protocol.shared.transport.TcpTransport;
import org.apache.directory.server.protocol.shared.transport.UdpTransport;
import org.apache.directory.server.xdbm.Index;
import org.apache.directory.shared.kerberos.KerberosTime;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.utils.Java;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Mini KDC based on Apache Directory Server that can be embedded in tests or used from command line
 * as a standalone KDC.
 *
 * MiniKdc sets 2 System properties when started and unsets them when stopped:
 *
 * - java.security.krb5.conf: set to the MiniKDC realm/host/port - sun.security.krb5.debug: set to
 * the debug value provided in the configuration
 *
 * As a result of this, multiple MiniKdc instances should not be started concurrently in the same
 * JVM.
 *
 * MiniKdc default configuration values are:
 *
 * - org.name=EXAMPLE (used to create the REALM) - org.domain=COM (used to create the REALM) -
 * kdc.bind.address=localhost - kdc.port=0 (ephemeral port) - instance=DefaultKrbServer -
 * max.ticket.lifetime=86400000 (1 day) - max.renewable.lifetime604800000 (7 days) - transport=TCP -
 * debug=false
 *
 * The generated krb5.conf forces TCP connections.
 *
 * Acknowledgements: this class is derived from the MiniKdc class in the hadoop-minikdc project (git
 * commit d8d8ed35f00b15ee0f2f8aaf3fe7f7b42141286b).
 */
public class MiniKdcWithLdapService {

  private static final Logger log = LoggerFactory.getLogger(MiniKdcWithLdapService.class);

  public static final String ORG_NAME = "org.name";
  public static final String ORG_DOMAIN = "org.domain";
  public static final String KDC_BIND_ADDRESS = "kdc.bind.address";
  public static final String KDC_PORT = "kdc.port";
  public static final String LDAP_BIND_ADDRESS = "ldap.bind.address";
  public static final String LDAP_PORT = "ldap.port";
  public static final String INSTANCE = "instance";
  public static final String MAX_TICKET_LIFETIME = "max.ticket.lifetime";
  public static final String MAX_RENEWABLE_LIFETIME = "max.renewable.lifetime";
  public static final String TRANSPORT = "transport";
  public static final String SSL_KEYSTORE_LOCATION = "ssl.keystore.location";
  public static final String SSL_KEYSTORE_PASSWORD = "ssl.keystore.password";
  public static final String SSL_TRUSTSTORE_LOCATION = "ssl.truststore.location";
  public static final String SSL_TRUSTSTORE_PASSWORD = "ssl.truststore.password";
  public static final String SASL_JAAS_CONFIG = "sasl.jaas.config";
  public static final String DEBUG = "debug";

  private static final String JAVA_SECURITY_KRB_5_CONF = "java.security.krb5.conf";
  private static final String SUN_SECURITY_KRB_5_DEBUG = "sun.security.krb5.debug";

  public enum LdapProtocol {
    LDAP,
    LDAPS;

    public String value() {
      return name().toLowerCase(Locale.ENGLISH);
    }
  }

  public enum LdapSecurityProtocol {
    PLAINTEXT,
    SSL;

    public String value() {
      return name().toLowerCase(Locale.ENGLISH);
    }
  }

  public enum LdapSecurityAuthentication {
    NONE,
    SIMPLE,
    GSSAPI
  }

  final private static Set<String> REQUIRED_PROPERTIES = Utils.mkSet(
      ORG_NAME,
      ORG_DOMAIN,
      KDC_BIND_ADDRESS,
      KDC_PORT,
      LDAP_BIND_ADDRESS,
      LDAP_PORT,
      INSTANCE,
      TRANSPORT);

  final private static Map<String, String> DEFAULT_CONFIG;

  static {
    DEFAULT_CONFIG = new HashMap<>();
    DEFAULT_CONFIG.put(KDC_BIND_ADDRESS, "localhost");
    DEFAULT_CONFIG.put(KDC_PORT, "0");
    DEFAULT_CONFIG.put(LDAP_BIND_ADDRESS, "localhost");
    DEFAULT_CONFIG.put(LDAP_PORT, "0");
    DEFAULT_CONFIG.put(INSTANCE, "DefaultLdapServer");
    DEFAULT_CONFIG.put(ORG_NAME, "Example");
    DEFAULT_CONFIG.put(ORG_DOMAIN, "COM");
    DEFAULT_CONFIG.put(TRANSPORT, "TCP");
    DEFAULT_CONFIG.put(MAX_TICKET_LIFETIME, "86400000");
    DEFAULT_CONFIG.put(MAX_RENEWABLE_LIFETIME, "604800000");
    DEFAULT_CONFIG.put(DEBUG, "false");
  }


  private final Properties config;
  private final File workDir;
  private final String orgName;
  private final String orgDomain;
  private final String realm;
  private final File krb5conf;

  private int kdcPort;
  private int ldapPort;

  private KdcServer kdc;
  private DirectoryService ds;
  private LdapServer ldapServer;
  private boolean closed;

  /**
   * Creates a new MiniLdapServer instance.
   *
   * @param config the MiniLdapServer configuration
   * @param workDir the working directory which will contain Apache DS files and any other files
   * needed by MiniLdapServer.
   */
  public MiniKdcWithLdapService(Properties config, File workDir) {
    this.config = config;
    this.workDir = workDir;

    if (!config.stringPropertyNames().containsAll(REQUIRED_PROPERTIES)) {
      Set<String> missingProperties = new HashSet<>(REQUIRED_PROPERTIES);
      missingProperties.removeAll(config.stringPropertyNames());
      throw new IllegalArgumentException("Missing configuration properties: " + missingProperties);
    }

    log.info("Configuration:");
    log.info("---------------------------------------------------------------");
    for (Map.Entry<Object, Object> entry : config.entrySet()) {
      log.info("\t{}: {}", entry.getKey(), entry.getValue());
    }
    log.info("---------------------------------------------------------------");

    orgName = config.getProperty(ORG_NAME);
    orgDomain = config.getProperty(ORG_DOMAIN);
    realm = orgName.toUpperCase(Locale.ENGLISH) + "." + orgDomain.toUpperCase(Locale.ENGLISH);
    krb5conf = new File(workDir, "krb5.conf");
    kdcPort = Integer.parseInt(config.getProperty(KDC_PORT));
    ldapPort = Integer.parseInt(config.getProperty(LDAP_PORT));
  }

  public int ldapPort() {
    return ldapPort;
  }

  public int kdcPort() {
    return kdcPort;
  }

  public String ldapHost() {
    return config.getProperty(LDAP_BIND_ADDRESS);
  }

  public String kdcHost() {
    return config.getProperty(KDC_BIND_ADDRESS);
  }

  public void start() {
    if (ds != null) {
      throw new RuntimeException("Directory server already started");
    }
    if (closed) {
      throw new RuntimeException("Directory server is closed");
    }
    try {
      initDirectoryService();
      initKdcServer();
      initJvmKerberosConfig();
    } catch (Exception e) {
      throw new RuntimeException("Directory server could not be started", e);
    }
  }

  private void initDirectoryService() throws Exception {
    ds = new DefaultDirectoryService();
    ds.setInstanceLayout(new InstanceLayout(workDir));
    ds.setCacheService(new CacheService());

    // first load the schema;
    InstanceLayout instanceLayout = ds.getInstanceLayout();
    File schemaPartitionDirectory = new File(instanceLayout.getPartitionsDirectory(), "schema");
    DefaultSchemaLdifExtractor extractor = new DefaultSchemaLdifExtractor(
        instanceLayout.getPartitionsDirectory());
    extractor.extractOrCopy();

    LdifSchemaLoader loader = new LdifSchemaLoader(schemaPartitionDirectory);
    DefaultSchemaManager schemaManager = new DefaultSchemaManager(loader);
    schemaManager.loadAllEnabled();
    ds.setSchemaManager(schemaManager);
    // Init the LdifPartition with schema;
    LdifPartition schemaLdifPartition = new LdifPartition(schemaManager, ds.getDnFactory());
    schemaLdifPartition.setPartitionPath(schemaPartitionDirectory.toURI());

    // The schema partition;
    SchemaPartition schemaPartition = new SchemaPartition(schemaManager);
    schemaPartition.setWrappedPartition(schemaLdifPartition);
    ds.setSchemaPartition(schemaPartition);

    JdbmPartition systemPartition = new JdbmPartition(ds.getSchemaManager(), ds.getDnFactory());
    systemPartition.setId("system");
    systemPartition.setPartitionPath(new File(ds.getInstanceLayout().getPartitionsDirectory(),
        systemPartition.getId()).toURI());
    systemPartition.setSuffixDn(new Dn(ServerDNConstants.SYSTEM_DN));
    systemPartition.setSchemaManager(ds.getSchemaManager());
    ds.setSystemPartition(systemPartition);

    ds.getChangeLog().setEnabled(false);
    ds.setDenormalizeOpAttrsEnabled(true);
    ds.addLast(new KeyDerivationInterceptor());

    // create one partition;
    String orgName = config.getProperty(ORG_NAME).toLowerCase(Locale.ENGLISH);
    String orgDomain = config.getProperty(ORG_DOMAIN).toLowerCase(Locale.ENGLISH);
    JdbmPartition partition = new JdbmPartition(ds.getSchemaManager(), ds.getDnFactory());
    partition.setId(orgName);
    partition.setPartitionPath(
        new File(ds.getInstanceLayout().getPartitionsDirectory(), orgName).toURI());
    Dn dn = new Dn(String.format("dc=%s,dc=%s", orgName, orgDomain));
    partition.setSuffixDn(dn);
    ds.addPartition(partition);

    // indexes
    Set<Index<?, String>> indexedAttributes = new HashSet<>();
    indexedAttributes.add(new JdbmIndex<Entry>("objectClass", false));
    indexedAttributes.add(new JdbmIndex<Entry>("dc", false));
    indexedAttributes.add(new JdbmIndex<Entry>("ou", false));
    partition.setIndexedAttributes(indexedAttributes);

    // And start the ds
    ds.setInstanceId(config.getProperty(INSTANCE));
    ds.startup();

    // context entry, after ds.startup()
    Entry entry = ds.newEntry(dn);
    entry.add("objectClass", "top", "domain");
    entry.add("dc", orgName);
    ds.getAdminSession().add(entry);

    startLdap(ldapPort);
  }

  public void startLdap(int port) throws Exception {
    // Start LDAP server
    ldapServer = new LdapServer();
    TcpTransport transport = new TcpTransport(port);
    if (sslEnabled()) {
      configureSsl(ldapServer, transport);
    }
    String auth = config.getProperty(Context.SECURITY_AUTHENTICATION);
    LdapSecurityAuthentication ldapAuth = auth == null ? LdapSecurityAuthentication.NONE
        : LdapSecurityAuthentication.valueOf(auth);
    switch (ldapAuth) {
      case GSSAPI:
        ldapServer.setSaslHost("localhost");
        ldapServer.setSaslPrincipal(String.format("ldap/localhost@%s.%s",
            orgName.toUpperCase(Locale.ENGLISH), orgDomain.toUpperCase(Locale.ENGLISH)));
        ldapServer.addSaslMechanismHandler("GSSAPI", new GssapiMechanismHandler());
        break;
      case SIMPLE:
        throw new IllegalArgumentException("Authentication method SIMPLE not supported");
      default:
        break;

    }
    ldapServer.setTransports(transport);
    ldapServer.setDirectoryService(ds);
    ds.setAllowAnonymousAccess(true);

    ldapServer.start();
    ldapPort = ((InetSocketAddress) ldapServer.getTransports()[0].getAcceptor().getLocalAddress())
        .getPort();
  }

  public void stopLdap() {
    ldapServer.stop();
  }

  private void addInitialEntriesToDirectoryService(String bindAddress)
      throws IOException, LdapException {
    Map<String, String> map = new HashMap<>(5);
    map.put("0", orgName.toLowerCase(Locale.ENGLISH));
    map.put("1", orgDomain.toLowerCase(Locale.ENGLISH));
    map.put("2", orgName.toUpperCase(Locale.ENGLISH));
    map.put("3", orgDomain.toUpperCase(Locale.ENGLISH));
    map.put("4", bindAddress);

    try (BufferedReader reader = resourceReader("/minikdcldap.ldiff")) {
      String line;
      StringBuilder builder = new StringBuilder();
      while ((line = reader.readLine()) != null) {
        builder.append(line).append("\n");
      }
      addEntriesToDirectoryService(StrSubstitutor.replace(builder, map));
    }
  }

  private void initKdcServer() throws IOException, LdapException {
    String bindAddress = config.getProperty(KDC_BIND_ADDRESS);
    addInitialEntriesToDirectoryService(bindAddress);

    KerberosConfig kerberosConfig = new KerberosConfig();
    kerberosConfig
        .setMaximumRenewableLifetime(Long.parseLong(config.getProperty(MAX_RENEWABLE_LIFETIME)));
    kerberosConfig
        .setMaximumTicketLifetime(Long.parseLong(config.getProperty(MAX_TICKET_LIFETIME)));
    kerberosConfig.setSearchBaseDn(String.format("dc=%s,dc=%s", orgName, orgDomain));
    kerberosConfig.setPaEncTimestampRequired(false);
    kdc = new KdcServer(kerberosConfig);
    kdc.setDirectoryService(ds);

    // transport
    String transport = config.getProperty(TRANSPORT).trim();
    AbstractTransport absTransport;
    if ("TCP".equals(transport)) {
      absTransport = new TcpTransport(bindAddress, kdcPort, 3, 50);
    } else if ("UDP".equals(transport)) {
      absTransport = new UdpTransport(kdcPort);
    } else {
      throw new IllegalArgumentException("Invalid transport: $transport");
    }
    kdc.addTransports(absTransport);
    kdc.setServiceName(config.getProperty(INSTANCE));
    kdc.start();

    // if using ephemeral port, update port number for binding
    if (kdcPort == 0) {
      kdcPort = ((InetSocketAddress) absTransport.getAcceptor().getLocalAddress()).getPort();
    }

    log.info("MiniKdc listening at port: {}", kdcPort);
  }

  private void initJvmKerberosConfig() throws Exception {
    writeKrb5Conf();
    System.setProperty(JAVA_SECURITY_KRB_5_CONF, krb5conf.getAbsolutePath());
    System.setProperty(SUN_SECURITY_KRB_5_DEBUG, config.getProperty(DEBUG, "false"));
    log.info("Setting JVM krb5.conf to: {}", krb5conf.getAbsolutePath());
    refreshJvmKerberosConfig();
  }

  private void writeKrb5Conf() throws IOException {
    StringBuilder stringBuilder = new StringBuilder();
    try (BufferedReader reader = resourceReader("/minikdcldap-krb5.conf")) {
      String line;
      while ((line = reader.readLine()) != null) {
        stringBuilder.append(line).append("{3}");
      }
    }
    String output = MessageFormat
        .format(stringBuilder.toString(), realm, kdcHost(), String.valueOf(kdcPort),
            System.lineSeparator());
    Files.write(krb5conf.toPath(), output.getBytes(StandardCharsets.UTF_8));
  }

  private void refreshJvmKerberosConfig() throws Exception {
    Class<?> klass = Java.isIbmJdk() ? Class.forName("com.ibm.security.krb5.internal.Config")
        : Class.forName("sun.security.krb5.Config");
    klass.getMethod("refresh").invoke(klass);
  }

  private boolean sslEnabled() {
    return LdapSecurityProtocol.SSL.name().toLowerCase(Locale.ENGLISH).equals(config.get(Context.SECURITY_PROTOCOL));
  }

  private void configureSsl(LdapServer ldapServer, TcpTransport transport) {
    ldapServer.setKeystoreFile(config.getProperty(SSL_KEYSTORE_LOCATION));
    ldapServer.setCertificatePassword(config.getProperty(SSL_KEYSTORE_PASSWORD));
    transport.setEnableSSL(true);
  }

  public void shutdown() {
    if (!closed) {
      closed = true;
      if (ldapServer != null) {
        ldapServer.stop();
      }
      if (kdc != null) {
        System.clearProperty(JAVA_SECURITY_KRB_5_CONF);
        System.clearProperty(SUN_SECURITY_KRB_5_DEBUG);
        kdc.stop();
        try {
          ds.shutdown();
        } catch (Exception ex) {
          log.error("Could not shutdown ApacheDS properly", ex);
        }
      }
    }
  }

  /**
   * Creates a principal in the KDC with the specified user and password.
   *
   * An exception will be thrown if the principal cannot be created.
   *
   * @param principal principal name, do not include the domain.
   * @param password password.
   */
  public void createPrincipal(String principal, String password) throws IOException, LdapException {
    StringBuilder ldifContent = new StringBuilder();
    ldifContent.append(String.format("dn: uid=%s,ou=users,dc=%s,dc=%s\n", principal,
        orgName.toLowerCase(Locale.ENGLISH), orgDomain.toLowerCase(Locale.ENGLISH)));
    ldifContent.append("objectClass: top\n");
    ldifContent.append("objectClass: person\n");
    ldifContent.append("objectClass: inetOrgPerson\n");
    ldifContent.append("objectClass: krb5principal\n");
    ldifContent.append("objectClass: krb5kdcentry\n");
    ldifContent.append("cn: ");
    ldifContent.append(principal);
    ldifContent.append("\n");
    ldifContent.append("sn: ");
    ldifContent.append(principal);
    ldifContent.append("\n");
    ldifContent.append("uid: ");
    ldifContent.append(principal);
    ldifContent.append("\n");
    ldifContent.append("userPassword: ");
    ldifContent.append(password);
    ldifContent.append("\n");
    ldifContent.append(String.format("krb5PrincipalName: %s@%s\n", principal, realm));
    ldifContent.append("krb5KeyVersionNumber: 0\n");
    addEntriesToDirectoryService(ldifContent.toString());
  }

  /**
   * Creates  multiple principals in the KDC and adds them to a keytab file.
   *
   * An exception will be thrown if the principal cannot be created.
   *
   * @param keytabFile keytab file to add the created principals
   * @param principals principals to add to the KDC, do not include the domain.
   */
  public void createPrincipal(File keytabFile, String... principals)
      throws IOException, LdapException {
    createPrincipal(keytabFile, Arrays.stream(principals)
        .collect(Collectors.toMap(Function.identity(), unused -> UUID.randomUUID().toString())));
  }

  public void createPrincipal(File keytabFile, Map<String, String> principalsWithPassword)
      throws IOException, LdapException {
    Keytab keytab = new Keytab();
    List<KeytabEntry> entries = principalsWithPassword.entrySet().stream().flatMap(entry -> {
      try {
        String principal = entry.getKey();
        String password = entry.getValue();
        createPrincipal(principal, password);
        String principalWithRealm = principal + "@" + realm;
        KerberosTime timestamp = new KerberosTime();
        return KerberosKeyFactory.getKerberosKeys(principalWithRealm, password)
            .values().stream().map(encryptionKey -> {
              byte keyVersion = (byte) encryptionKey.getKeyVersion();
              return new KeytabEntry(principalWithRealm, 1, timestamp, keyVersion, encryptionKey);
            });
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }).collect(Collectors.toList());
    keytab.setEntries(entries);
    keytab.write(keytabFile);
  }

  /**
   * Creates a user in the directory server. <p> An exception will be thrown if the user cannot be
   * created.
   *
   * @param userName User name (uid)
   */
  public void createUser(String userName) throws IOException, LdapException {
    String ldifContent = "dn: " + userDn(userName) + "\n"
        + "objectClass: top\n"
        + "objectClass: person\n"
        + "objectClass: inetOrgPerson\n"
        + "cn: " + userName + "\n"
        + "sn: " + userName + "\n"
        + "uid: " + userName + "\n";
    addEntriesToDirectoryService(ldifContent);
  }

  /**
   * Creates a group in the directory server.; <p> An exception will be thrown if the group cannot
   * be created.;
   *
   * @param group Group name;
   * @param users DN of users that belong to the group;
   */
  public void createGroup(String group, String... users) throws LdapException, IOException {
    StringBuilder ldifContent = new StringBuilder();
    ldifContent.append("\n");
    ldifContent.append("dn: ");
    ldifContent.append(groupDn(group));
    ldifContent.append("\n");
    ldifContent.append("objectClass: top\n");
    ldifContent.append("objectClass: groupOfNames\n");
    ldifContent.append("cn: ");
    ldifContent.append(group);
    ldifContent.append("\n");
    for (String user : users) {
      ldifContent.append("member: ");
      ldifContent.append(userDn(user));
      ldifContent.append("\n");
    }
    addEntriesToDirectoryService(ldifContent.toString());
  }

  private String userDn(String userName) {
    return String.format("uid=%s,ou=users,dc=%s,dc=%s", userName,
        orgName.toLowerCase(Locale.ENGLISH), orgDomain.toLowerCase(Locale.ENGLISH));
  }

  private String groupDn(String group) {
    return String.format("cn=%s,ou=groups,dc=%s,dc=%s", group,
        orgName.toLowerCase(Locale.ENGLISH), orgDomain.toLowerCase(Locale.ENGLISH));
  }

  public void deleteGroup(String group) throws LdapException {
    ds.getAdminSession().delete(new Dn(ds.getSchemaManager(), groupDn(group)));
  }

  public void deleteUser(String user) throws LdapException {
    ds.getAdminSession().delete(new Dn(ds.getSchemaManager(), userDn(user)));
  }

  public void renameGroup(String oldGroup, String newGroup) throws LdapException, InvalidNameException {
    String oldDn = groupDn(oldGroup);
    String newRdn = String.format("cn=%s", newGroup);
    ds.getAdminSession().rename(new Dn(ds.getSchemaManager(), oldDn), new Rdn(newRdn), true);
  }

  public void renameUser(String oldUser, String newUser) throws LdapException, InvalidNameException {
    String oldDn = userDn(oldUser);
    String newRdn = String.format("uid=%s", newUser);
    ds.getAdminSession().rename(new Dn(ds.getSchemaManager(), oldDn), new Rdn(newRdn), true);
  }

  public void addUserToGroup(String group, String user) throws LdapException {
    alterUserGroup(group, user, ModificationOperation.ADD_ATTRIBUTE);
  }

  public void removeUserFromGroup(String group, String user) throws LdapException {
    alterUserGroup(group, user, ModificationOperation.REMOVE_ATTRIBUTE);
  }

  private void alterUserGroup(String group, String user, ModificationOperation op)
      throws LdapException {
    CoreSession session = ds.getAdminSession();
    String groupDn = groupDn(group);
    Attribute attr = attribute("member", userDn(user));
    session.modify(new Dn(ds.getSchemaManager(), groupDn), new DefaultModification(op, attr));
  }

  private Attribute attribute(String name, String value) throws LdapException {
    AttributeType attrType = ds.getSchemaManager().lookupAttributeTypeRegistry(name);
    return new DefaultAttribute(attrType, value);
  }

  private String ldapProviderUrl() {
    LdapProtocol protocol = sslEnabled() ? LdapProtocol.LDAPS : LdapProtocol.LDAP;
    return String.format("%s://%s:%d/dc=%s,dc=%s", protocol.value(), ldapHost(), ldapPort(),
        orgName.toLowerCase(Locale.ENGLISH), orgDomain.toLowerCase(Locale.ENGLISH));
  }

  public Map<String, String> ldapClientConfigs() {
    Map<String, String> props = new HashMap<>();
    props.put(Context.PROVIDER_URL, ldapProviderUrl());
    if (sslEnabled()) {
      props.put(SSL_TRUSTSTORE_LOCATION, config.getProperty(SSL_KEYSTORE_LOCATION));
      props.put(SSL_TRUSTSTORE_PASSWORD, config.getProperty(SSL_KEYSTORE_PASSWORD));
      props.put(Context.SECURITY_PROTOCOL, LdapSecurityProtocol.SSL.value());
    }
    String auth = config.getProperty(Context.SECURITY_AUTHENTICATION);
    if (auth != null) {
      props.put(Context.SECURITY_AUTHENTICATION, auth);
    }
    if (LdapSecurityAuthentication.GSSAPI.name().equals(auth)) {
      props.put(SASL_JAAS_CONFIG, config.getProperty(SASL_JAAS_CONFIG));
      props.put(Context.SECURITY_PRINCIPAL, String.format("%s@%s.%s", "ldap",
          orgName.toLowerCase(Locale.ENGLISH), orgDomain.toLowerCase(Locale.ENGLISH)));
    }
    return props;
  }

  private void addEntriesToDirectoryService(String ldifContent) throws LdapException, IOException {
    try (LdifReader reader = new LdifReader(new StringReader(ldifContent))) {
      for (LdifEntry ldifEntry : reader) {
        ds.getAdminSession().add(new DefaultEntry(ds.getSchemaManager(), ldifEntry.getEntry()));
      }
    }
  }

  public static void main(String[] args) throws IOException, LdapException {
    if (args.length < 4) {
      System.err.println(
          "Arguments: <WORKDIR> <MINILDAPSERVERPROPERTIES> <KEYTABFILE> [<PRINCIPAL:PASSWORD:GROUP*>]+");
      System.err.println("For example:");
      System.err.println("java MiniKdcWithLdapService /tmp /tmp/minikdc.conf /tmp/test.keytab alice:alice-secret:Finance:Admin bob:bob-secret:Admin");
      System.exit(1);
    }

    String workDirPath = args[0];
    String configPath = args[1];
    File workDir = new File(workDirPath);
    if (!workDir.exists()) {
      throw new RuntimeException(
          "Specified work directory does not exist: " + workDir.getAbsolutePath());
    }
    Properties config = createConfig();
    File configFile = new File(configPath);
    if (!configFile.exists()) {
      throw new RuntimeException(
          "Specified configuration does not exist: " + configFile.getAbsolutePath());
    }

    Properties userConfig = Utils.loadProps(configFile.getAbsolutePath());
    config.putAll(userConfig);

    String keytabPath = args[2];
    File keytabFile = new File(keytabPath).getAbsoluteFile();
    Map<String, UserMetadata> principals = new HashMap<>();
    for (int i = 3; i < args.length; i++) {
      String[] principalAndGroups = args[i].split(":");
      List<String> groups = new ArrayList<>();
      for (int j = 2; j < principalAndGroups.length; j++) {
        groups.add(principalAndGroups[j]);
      }
      String password = principalAndGroups.length < 2 ? UUID.randomUUID().toString() : principalAndGroups[1];
      principals.put(principalAndGroups[0], new UserMetadata(password, groups));
    }
    start(workDir, config, keytabFile, principals);
  }

  private static void start(File workDir, Properties config, File keytabFile,
      Map<String, UserMetadata> principalsWithMetadata) throws LdapException, IOException {
    MiniKdcWithLdapService miniKdc = new MiniKdcWithLdapService(config, workDir);
    miniKdc.start();

    Map<String, List<String>> groups = new HashMap<>();
    Map<String, String> users = new HashMap<>(principalsWithMetadata.size());
    for (Map.Entry<String, UserMetadata> entry : principalsWithMetadata.entrySet()) {
      String user = entry.getKey();
      users.put(user, entry.getValue().password);
      for (String group : entry.getValue().groups) {
        groups.computeIfAbsent(group, g -> new ArrayList<>()).add(user);
      }
    }
    miniKdc.createPrincipal(keytabFile, users);
    for (Map.Entry<String, List<String>> entry : groups.entrySet()) {
      String group = entry.getKey();
      String[] members = entry.getValue().toArray(new String[entry.getValue().size()]);
      miniKdc.createGroup(group, members);
    }

    System.out.println();
    System.out.println("Standalone MiniLdapServer Running");
    System.out.println("---------------------------------------------------");
    System.out.println("  Running at      : " + miniKdc.kdcHost() + ":" + miniKdc.kdcPort());
    System.out.println("  created keytab  : " + keytabFile);
    System.out.println("  Running LDAP at : " + miniKdc.ldapHost() + ":" + miniKdc.ldapPort());
    System.out.println("  with principals->groups : " + principalsWithMetadata);
    System.out.println("  with groups->principals : " + groups);
    System.out.println();
    System.out.println("Hit <CTRL-C> or kill <PID> to stop it");
    System.out.println("---------------------------------------------------");
    System.out.println();

    Runtime.getRuntime().addShutdownHook(new Thread(miniKdc::shutdown));
  }

  /**
   * Convenience method that returns MiniLdapServer default configuration. <p> The returned
   * configuration is a copy, it can be customized before using it to create a MiniLdapServer.
   */
  public static Properties createConfig() {
    Properties properties = new Properties();
    properties.putAll(DEFAULT_CONFIG);
    return properties;
  }

  public static void addSslConfig(Properties config, Properties sslConfigs) {
    config.put(SSL_KEYSTORE_LOCATION, sslConfigs.getProperty(SSL_KEYSTORE_LOCATION));
    config.put(SSL_KEYSTORE_PASSWORD, ((Password) sslConfigs.get(SSL_KEYSTORE_PASSWORD)).value());
    config.put(Context.SECURITY_PROTOCOL, LdapSecurityProtocol.SSL.value());
  }

  public static void addGssapiConfig(Properties config, String saslJaasConfig) {
    config.put(SASL_JAAS_CONFIG, saslJaasConfig);
    config.put(Context.SECURITY_AUTHENTICATION, LdapSecurityAuthentication.GSSAPI.name());
  }

  private static BufferedReader resourceReader(String resourceName) throws IOException {
    InputStream in =  MiniKdcWithLdapService.class.getResourceAsStream(resourceName);
    return new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
  }

  private static class UserMetadata {
    final String password;
    final List<String> groups;

    UserMetadata(String password, List<String> groups) {
      this.password = password;
      this.groups = groups;
    }

    @Override
    public String toString() {
      return groups.toString();
    }
  }
}
