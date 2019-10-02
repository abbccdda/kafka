/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.crn;

import static org.apache.kafka.common.resource.ResourcePattern.WILDCARD_RESOURCE;

import com.google.common.base.CaseFormat;
import io.confluent.crn.ConfluentResourceName.Element;
import io.confluent.security.authorizer.ResourcePattern;
import io.confluent.security.authorizer.Scope;
import io.confluent.security.authorizer.Scope.Builder;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.utils.Utils;

/**
 * This CrnAuthority handles the resource types present in the Confluent Platform
 */
public class ConfluentServerCrnAuthority implements CrnAuthority, Configurable {

  // These are the strings that appear in the CRN types
  public static final String ORGANIZATION_TYPE = "organization";
  public static final String ENVIRONMENT_TYPE = "environment";
  public static final String KAFKA_CLUSTER_TYPE = "kafka";
  public static final String KSQL_CLUSTER_TYPE = "ksql";
  public static final String CONNECT_CLUSTER_TYPE = "connect";
  public static final String SCHEMA_REGISTRY_CLUSTER_TYPE = "schema-registry";

  // These are the strings that appear as keys in the Scope clusters map
  public static final String KAFKA_CLUSTER_KEY = "kafka-cluster";
  public static final String KSQL_CLUSTER_KEY = "ksql-cluster";
  public static final String CONNECT_CLUSTER_KEY = "connect-cluster";
  public static final String SCHEMA_REGISTRY_CLUSTER_KEY = "schema-registry-cluster";

  // These are the strings that appear as ResourceTypes in RBAC
  public static final String KAFKA_CLUSTER_RESOURCE_TYPE = "Cluster";
  public static final String KSQL_CLUSTER_RESOURCE_TYPE = "KsqlCluster";
  // These are extrapolated from the strings that appear as ResourceTypes in RBAC
  public static final String CONNECT_CLUSTER_RESOURCE_TYPE = "ConnectCluster";
  public static final String SCHEMA_REGISTRY_CLUSTER_RESOURCE_TYPE = "SchemaRegistryCluster";

  public static final Set<String> CLUSTER_RESOURCE_TYPES =
      Utils.mkSet(KAFKA_CLUSTER_RESOURCE_TYPE, KSQL_CLUSTER_RESOURCE_TYPE,
          CONNECT_CLUSTER_RESOURCE_TYPE, SCHEMA_REGISTRY_CLUSTER_RESOURCE_TYPE);

  public static final Map<String, String> CLUSTER_KEY_BY_TYPE =
      Utils.mkMap(
          Utils.mkEntry(KAFKA_CLUSTER_TYPE, KAFKA_CLUSTER_KEY),
          Utils.mkEntry(KSQL_CLUSTER_TYPE, KSQL_CLUSTER_KEY),
          Utils.mkEntry(CONNECT_CLUSTER_TYPE, CONNECT_CLUSTER_KEY),
          Utils.mkEntry(SCHEMA_REGISTRY_CLUSTER_TYPE, SCHEMA_REGISTRY_CLUSTER_KEY));

  public static final Map<String, String> CLUSTER_TYPE_BY_KEY =
      CLUSTER_KEY_BY_TYPE.entrySet().stream()
          .collect(Collectors.toMap(Entry::getValue, Entry::getKey));

  public static final Map<String, String> CLUSTER_RESOURCE_TYPE_BY_TYPE =
      Utils.mkMap(
          Utils.mkEntry(KAFKA_CLUSTER_TYPE, KAFKA_CLUSTER_RESOURCE_TYPE),
          Utils.mkEntry(KSQL_CLUSTER_TYPE, KSQL_CLUSTER_RESOURCE_TYPE),
          Utils.mkEntry(CONNECT_CLUSTER_TYPE, CONNECT_CLUSTER_RESOURCE_TYPE),
          Utils.mkEntry(SCHEMA_REGISTRY_CLUSTER_TYPE, SCHEMA_REGISTRY_CLUSTER_RESOURCE_TYPE));

  private String authorityName;
  private int cacheCapacity;

  private static class Result<T, E extends Exception> {
    final T value;
    final E exception;

    public Result(T value) {
      this.value = value;
      this.exception = null;
    }

    public Result(E exception) {
      this.value = null;
      this.exception = exception;
    }

    public T get() throws E {
      if (exception != null) {
        throw exception;
      }
      return value;
    }
  }

  private final Map<String, Result<ConfluentResourceName, CrnSyntaxException>> crnCacheByString;
  private final Map<ScopedResourcePattern, Result<ConfluentResourceName, CrnSyntaxException>> crnCacheByScopedResourcePattern;

  public ConfluentServerCrnAuthority(String authorityName, int initialCacheCapacity) {
    this.authorityName = authorityName;
    this.cacheCapacity = initialCacheCapacity;
    this.crnCacheByString = Collections.synchronizedMap(
        new LinkedHashMap<String, Result<ConfluentResourceName, CrnSyntaxException>>(
            cacheCapacity, 0.75f, true) {
          protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > cacheCapacity;
          }
        });
    this.crnCacheByScopedResourcePattern = Collections.synchronizedMap(
        new LinkedHashMap<ScopedResourcePattern, Result<ConfluentResourceName, CrnSyntaxException>>(
            cacheCapacity, 0.75f, true) {
          protected boolean removeEldestEntry(Map.Entry eldest) {
            return size() > cacheCapacity;
          }
        });
  }

  public ConfluentServerCrnAuthority() {
    this(null, CrnAuthorityConfig.CACHE_ENTRIES_DEFAULT);
  }

  private String toCrnResourceType(String rbacResourceName) {
    return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_HYPHEN, rbacResourceName);
  }

  private String toPatternResourceType(String crnResourceName) {
    return CaseFormat.LOWER_HYPHEN.to(CaseFormat.UPPER_CAMEL, crnResourceName);
  }

  @Override
  public String name() {
    return authorityName;
  }

  public ScopedResourcePattern resolveScopePattern(ConfluentResourceName crn)
      throws CrnSyntaxException {
    ResourcePattern resourcePattern = null;
    Scope.Builder scopeBuilder = new Builder();

    if (!authorityName.equals(crn.authority())) {
      throw new CrnSyntaxException(crn.toString(), "Wrong authority. Expected: " + authorityName);
    }

    List<Element> elements = crn.elements();
    for (ConfluentResourceName.Element e : elements) {
      String type = e.resourceType();
      switch (type) {
        case ORGANIZATION_TYPE:
        case ENVIRONMENT_TYPE:
          scopeBuilder.addPath(e.encodedResourceName());
          break;
        case KAFKA_CLUSTER_TYPE:
        case KSQL_CLUSTER_TYPE:
        case CONNECT_CLUSTER_TYPE:
        case SCHEMA_REGISTRY_CLUSTER_TYPE:
          scopeBuilder.withCluster(CLUSTER_KEY_BY_TYPE.get(type), e.encodedResourceName());
          break;
        default:
          if (resourcePattern != null) {
            throw new CrnSyntaxException(crn.toString(),
                String.format("Found multiple resources: %s=%s and %s=%s",
                    resourcePattern.resourceType(), resourcePattern.name(),
                    e.resourceType(), e.encodedResourceName()));
          }
          String resourceName;
          PatternType patternType;

          resourceName = e.encodedResourceName();
          if (resourceName.equals(WILDCARD_RESOURCE)) {
            patternType = PatternType.ANY;
            resourceName = "";
          } else if (resourceName.endsWith(WILDCARD_RESOURCE)) {
            patternType = PatternType.PREFIXED;
            resourceName = resourceName.substring(0, resourceName.length() - 1);
          } else {
            patternType = PatternType.LITERAL;
          }

          try {
            resourceName = URLDecoder.decode(resourceName, StandardCharsets.UTF_8.name());
          } catch (UnsupportedEncodingException ex) {
            // This should never happen, since we're using the predefined charset
            throw new RuntimeException(ex);
          }

          resourcePattern = new ResourcePattern(toPatternResourceType(e.resourceType()),
              resourceName, patternType);
          break;
      }
    }
    // If there's no non-cluster resource (topic, connector, etc.), this is a cluster CRN
    if (resourcePattern == null) {
      Element lastCluster = elements.get(elements.size() - 1);
      String type = lastCluster.resourceType();
      resourcePattern = new ResourcePattern(
          CLUSTER_RESOURCE_TYPE_BY_TYPE.get(type),
          CLUSTER_KEY_BY_TYPE.get(type),
          PatternType.LITERAL
      );
    }

    return new ScopedResourcePattern(scopeBuilder.build(), resourcePattern);
  }

  private ConfluentResourceName uncachedCanonicalCrn(Scope scope, ResourcePattern resourcePattern)
      throws CrnSyntaxException {
    ConfluentResourceName.Builder builder = ConfluentResourceName.newBuilder();
    builder.setAuthority(authorityName);

    // Arbitrarily, we consider the innermost scope an Environment and all others Organizations
    List<String> path = scope.path();
    for (int i = 0; i < path.size() - 1; i++) {
      builder.addElement(ORGANIZATION_TYPE, path.get(i));
    }
    if (path.size() > 0) {
      builder.addElement(ENVIRONMENT_TYPE, path.get(path.size() - 1));
    }

    // Kafka Cluster must be the first, because it's the one we know has a unique ID
    Map<String, String> clusters = scope.clusters();
    if (clusters.containsKey(KAFKA_CLUSTER_KEY)) {
      builder.addElement(KAFKA_CLUSTER_TYPE, clusters.get(KAFKA_CLUSTER_KEY));
    } else {
      throw new CrnSyntaxException("", "Kafka cluster must be present");
    }
    ArrayList<CrnSyntaxException> exceptions = new ArrayList<>();
    clusters.entrySet().stream()
        .filter(e -> !KAFKA_CLUSTER_KEY.equals(e.getKey()))
        .sorted(Comparator.comparing(Entry::getKey))
        .forEach(e -> {
          try {
            String clusterType = CLUSTER_TYPE_BY_KEY.get(e.getKey());
            if (clusterType != null) {
              builder.addElement(clusterType, e.getValue());
            } else {
              exceptions.add(new CrnSyntaxException(e.getKey(), "Unknown cluster type"));
            }
          } catch (CrnSyntaxException exception) {
            exceptions.add(exception);
          }
        });
    if (!exceptions.isEmpty()) {
      throw new CrnSyntaxException("", exceptions);
    }

    // see if we need to add a resource or if the cluster was specified above
    if (resourcePattern != null &&
        !CLUSTER_RESOURCE_TYPES.contains(resourcePattern.resourceType().name())) {
      String resourceType = toCrnResourceType(resourcePattern.resourceType().name());
      String resourceName = resourcePattern.name();
      if (resourcePattern.patternType() == PatternType.ANY) {
        builder.addElementWithWildcard(resourceType, "");
      } else if (resourcePattern.patternType() == PatternType.PREFIXED) {
        builder.addElementWithWildcard(resourceType, resourceName);
      } else {
        builder.addElement(resourceType, resourceName);
      }
    }
    return builder.build();
  }

  public ConfluentResourceName canonicalCrn(Scope scope, ResourcePattern resourcePattern)
      throws CrnSyntaxException {
    ScopedResourcePattern scopedResourcePattern = new ScopedResourcePattern(scope, resourcePattern);
    Result<ConfluentResourceName, CrnSyntaxException> result = crnCacheByScopedResourcePattern
        .computeIfAbsent(scopedResourcePattern, srp -> {
          try {
            return new Result<>(uncachedCanonicalCrn(srp.scope(), srp.resourcePattern()));
          } catch (CrnSyntaxException e) {
            return new Result<>(e);
          }
        });
    return result.get();
  }


  public ConfluentResourceName canonicalCrn(Scope scope) throws CrnSyntaxException {
    return canonicalCrn(scope, null);
  }

  @Override
  public ConfluentResourceName canonicalCrn(String crnString) throws CrnSyntaxException {
    Result<ConfluentResourceName, CrnSyntaxException> result = crnCacheByString.computeIfAbsent(crnString, s -> {
      try {
        ScopedResourcePattern resolved = resolveScopePattern(
            ConfluentResourceName.fromString(crnString));
        return new Result<>(canonicalCrn(resolved.scope(), resolved.resourcePattern()));
      } catch (CrnSyntaxException e) {
        return new Result<>(e);
      }
    });
    return result.get();
  }

  @Override
  public ConfluentResourceName canonicalCrn(ConfluentResourceName crn) throws CrnSyntaxException {
    ScopedResourcePattern resolved = resolveScopePattern(crn);
    return canonicalCrn(resolved.scope(), resolved.resourcePattern());
  }

  @Override
  public boolean areEquivalent(ConfluentResourceName a, ConfluentResourceName b)
      throws CrnSyntaxException {
    return canonicalCrn(a).equals(canonicalCrn(b));
  }

  @Override
  public void configure(Map<String, ?> configs) {
    CrnAuthorityConfig config = new CrnAuthorityConfig(configs);
    this.authorityName = config.getString(CrnAuthorityConfig.AUTHORITY_NAME_PROP);
    this.cacheCapacity = config.getInt(CrnAuthorityConfig.CACHE_ENTRIES_PROP);
  }
}
