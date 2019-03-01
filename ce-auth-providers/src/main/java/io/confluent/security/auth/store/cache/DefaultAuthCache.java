// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.store.cache;

import io.confluent.kafka.security.authorizer.Operation;
import io.confluent.kafka.security.authorizer.Resource;
import io.confluent.kafka.security.authorizer.AccessRule;
import io.confluent.kafka.security.authorizer.PermissionType;
import io.confluent.kafka.security.authorizer.ResourceType;
import io.confluent.kafka.security.authorizer.provider.InvalidScopeException;
import io.confluent.security.auth.metadata.AuthCache;
import io.confluent.security.auth.store.data.AuthEntryType;
import io.confluent.security.auth.store.data.AuthKey;
import io.confluent.security.auth.store.data.AuthValue;
import io.confluent.security.auth.store.data.StatusKey;
import io.confluent.security.auth.store.data.StatusValue;
import io.confluent.security.auth.store.data.RoleAssignmentKey;
import io.confluent.security.auth.store.data.RoleAssignmentValue;
import io.confluent.security.auth.store.data.UserKey;
import io.confluent.security.auth.store.data.UserValue;
import io.confluent.security.rbac.AccessPolicy;
import io.confluent.security.rbac.RbacRoles;
import io.confluent.security.rbac.Role;
import io.confluent.security.rbac.RoleAssignment;
import io.confluent.security.rbac.Scope;
import io.confluent.security.rbac.UserMetadata;
import io.confluent.security.store.KeyValueStore;
import io.confluent.security.store.MetadataStoreException;
import io.confluent.security.store.MetadataStoreStatus;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.stream.Collectors;
import org.apache.kafka.common.record.InvalidRecordException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cache containing authorization and authentication metadata. This is obtained from
 * a Kafka metadata topic.
 *
 * Assumptions:
 * <ul>
 *   <li>Updates are on a single thread, but access policies and assignments may be read
 *   from different threads concurrently.</li>
 *   <li>Single-writer model ensures that we can perform updates and deletes at resource level
 *   for role assignments, for example to add a resource to an existing role assignment.</li>
 * </ul>
 */
public class DefaultAuthCache implements AuthCache, KeyValueStore<AuthKey, AuthValue> {
  private static final Logger log = LoggerFactory.getLogger(DefaultAuthCache.class);

  private static final String WILDCARD_HOST = "*";
  private static final NavigableMap<Resource, Set<AccessRule>> NO_RULES = Collections.emptyNavigableMap();

  private final RbacRoles rbacRoles;
  private final Scope rootScope;
  private final Map<KafkaPrincipal, UserMetadata> users;
  private final Map<RoleAssignmentKey, RoleAssignmentValue> roleAssignments;
  private final Map<Scope, Set<KafkaPrincipal>> rbacSuperUsers;
  private final Map<Scope, NavigableMap<Resource, Set<AccessRule>>> rbacAccessRules;
  private final Map<Integer, StatusValue> partitionStatus;

  public DefaultAuthCache(RbacRoles rbacRoles, Scope rootScope) {
    this.rbacRoles = rbacRoles;
    this.rootScope = rootScope;
    this.users = new ConcurrentHashMap<>();
    this.roleAssignments = new ConcurrentHashMap<>();
    this.rbacSuperUsers = new ConcurrentHashMap<>();
    this.rbacAccessRules = new ConcurrentHashMap<>();
    this.partitionStatus = new ConcurrentHashMap<>();
  }

  /**
   * Returns true if the provided user principal or any of the group principals has
   * `Super User` role at the specified scope.
   *
   * @param scope Scope being checked, super-users are parent level also return true
   * @param userPrincipal User principal
   * @param groupPrincipals Set of group principals of the user
   * @return true if the provided principal is a super user or super group.
   */
  @Override
  public boolean isSuperUser(Scope scope,
                             KafkaPrincipal userPrincipal,
                             Collection<KafkaPrincipal> groupPrincipals) {
    ensureNotFailed();
    if (!this.rootScope.containsScope(scope))
      throw new InvalidScopeException("This authorization cache does not contain scope " + scope);

    Set<KafkaPrincipal> matchingPrincipals = matchingPrincipals(userPrincipal, groupPrincipals);
    Scope nextScope = scope;
    while (nextScope != null) {
      Set<KafkaPrincipal> superUsers = rbacSuperUsers.getOrDefault(nextScope, Collections.emptySet());
      if (superUsers.stream().anyMatch(matchingPrincipals::contains))
        return true;
      nextScope = nextScope.parent();
    }
    return false;
  }

  /**
   * Returns the groups of the provided user principal.
   * @param userPrincipal User principal
   * @return Set of group principals of the user, which may be empty
   */
  @Override
  public Set<KafkaPrincipal> groups(KafkaPrincipal userPrincipal) {
    ensureNotFailed();
    UserMetadata user = users.get(userPrincipal);
    return user == null ? Collections.emptySet() : user.groups();
  }

  /**
   * Returns the RBAC rules corresponding to the provided principals that match
   * the specified resource.
   *
   * @param resourceScope Scope of the resource
   * @param resource Resource pattern to match
   * @param userPrincipal User principal
   * @param groupPrincipals Set of group principals of the user
   * @return Set of access rules that match the principals and resource
   */
  @Override
  public Set<AccessRule> rbacRules(Scope resourceScope,
                                   Resource resource,
                                   KafkaPrincipal userPrincipal,
                                   Collection<KafkaPrincipal> groupPrincipals) {
    ensureNotFailed();
    if (!this.rootScope.containsScope(resourceScope))
      throw new InvalidScopeException("This authorization cache does not contain scope " + resourceScope);

    Set<KafkaPrincipal> matchingPrincipals = matchingPrincipals(userPrincipal, groupPrincipals);

    Set<AccessRule> resourceRules = new HashSet<>();
    Scope nextScope = resourceScope;
    while (nextScope != null) {
      NavigableMap<Resource, Set<AccessRule>> rules = rbacRules(nextScope);
      if (rules != null) {
        String resourceName = resource.name();
        ResourceType resourceType = resource.resourceType();

        addMatchingRules(rules.get(resource), resourceRules, matchingPrincipals);
        addMatchingRules(rules.get(Resource.all(resourceType)), resourceRules, matchingPrincipals);
        addMatchingRules(rules.get(Resource.ALL), resourceRules, matchingPrincipals);

        rules.subMap(
            new Resource(resourceType.name(), resourceName, PatternType.PREFIXED), true,
            new Resource(resourceType.name(), resourceName.substring(0, 1), PatternType.PREFIXED), true)
            .entrySet().stream()
            .filter(e -> resourceName.startsWith(e.getKey().name()))
            .forEach(e -> addMatchingRules(e.getValue(), resourceRules, matchingPrincipals));
      }
      nextScope = nextScope.parent();
    }
    return resourceRules;
  }

  private void addMatchingRules(Collection<AccessRule> inputRules,
                                Collection<AccessRule> outputRules,
                                Set<KafkaPrincipal> principals) {
    if (inputRules != null)
      inputRules.stream().filter(r -> principals.contains(r.principal())).forEach(outputRules::add);
  }

  private Set<KafkaPrincipal> matchingPrincipals(KafkaPrincipal userPrincipal,
                                                 Collection<KafkaPrincipal> groupPrincipals) {
    HashSet<KafkaPrincipal> principals = new HashSet<>(groupPrincipals.size() + 1);
    principals.addAll(groupPrincipals);
    principals.add(userPrincipal);
    return principals;
  }

  @Override
  public Set<RoleAssignment> rbacRoleAssignments(Scope scope) {
    ensureNotFailed();
    Set<RoleAssignment> assignments = new HashSet<>();
    roleAssignments.entrySet().stream()
        .filter(e -> scope.name().equals(e.getKey().scope()))
        .forEach(e -> {
          RoleAssignmentKey key = e.getKey();
          Collection<Resource> resources = e.getValue().resources();
          assignments.add(new RoleAssignment(key.principal(), key.role(), key.scope(), resources));
        });
    return assignments;
  }

  @Override
  public UserMetadata userMetadata(KafkaPrincipal userPrincipal) {
    return users.get(userPrincipal);
  }

  public Map<KafkaPrincipal, UserMetadata> users() {
    return Collections.unmodifiableMap(users);
  }

  @Override
  public Scope rootScope() {
    return rootScope;
  }

  @Override
  public RbacRoles rbacRoles() {
    return rbacRoles;
  }

  @Override
  public AuthValue get(AuthKey key) {
    switch (key.entryType()) {
      case ROLE_ASSIGNMENT:
        RoleAssignmentKey roleAssignmentKey = (RoleAssignmentKey) key;
        return roleAssignments.get(roleAssignmentKey);
      case USER:
        UserMetadata user = users.get(((UserKey) key).principal());
        return user == null ? null : new UserValue(user.groups());
      case STATUS:
        StatusKey statusKey = (StatusKey) key;
        return partitionStatus.get(statusKey.partition());
      default:
        throw new IllegalArgumentException("Unknown key type " + key.entryType());
    }
  }
  @Override
  public AuthValue put(AuthKey key, AuthValue value) {
    if (value == null)
      throw new IllegalArgumentException("Value must not be null");
    if (key.entryType() != value.entryType())
      throw new InvalidRecordException("Invalid record with key=" + key + ", value=" + value);
    switch (key.entryType()) {
      case ROLE_ASSIGNMENT:
        return updateRoleAssignment((RoleAssignmentKey) key, (RoleAssignmentValue) value);
      case USER:
        return updateUser((UserKey) key, (UserValue) value);
      case STATUS:
        return partitionStatus.put(((StatusKey) key).partition(), (StatusValue) value);
      default:
        throw new IllegalArgumentException("Unknown key type " + key.entryType());
    }
  }

  @Override
  public AuthValue remove(AuthKey key) {
    switch (key.entryType()) {
      case ROLE_ASSIGNMENT:
        return removeRoleAssignment((RoleAssignmentKey) key);
      case USER:
        UserMetadata oldUser = users.remove(((UserKey) key).principal());
        return oldUser == null ? null : new UserValue(oldUser.groups());
      case STATUS:
        return partitionStatus.remove(((StatusKey) key).partition());
      default:
        throw new IllegalArgumentException("Unknown key type " + key.entryType());
    }
  }

  @Override
  public Map<? extends AuthKey, ? extends AuthValue> map(String type) {
    AuthEntryType entryType = AuthEntryType.valueOf(type);
    switch (entryType) {
      case ROLE_ASSIGNMENT:
        return Collections.unmodifiableMap(roleAssignments);
      case USER:
        return users.entrySet().stream()
            .collect(Collectors.toMap(e -> new UserKey(e.getKey()), e -> new UserValue(e.getValue().groups())));
      case STATUS:
        return partitionStatus.entrySet().stream()
            .collect(Collectors.toMap(e -> new StatusKey(e.getKey()), Map.Entry::getValue));
      default:
        throw new IllegalArgumentException("Unknown key type " + entryType);
    }
  }

  @Override
  public void fail(int partition, String errorMessage) {
    partitionStatus.put(partition, new StatusValue(MetadataStoreStatus.FAILED, -1, errorMessage));
  }

  @Override
  public MetadataStoreStatus status(int partition) {
    StatusValue statusValue = partitionStatus.get(partition);
    return statusValue != null ? statusValue.status() : MetadataStoreStatus.UNKNOWN;
  }

  private RoleAssignmentValue updateRoleAssignment(RoleAssignmentKey key, RoleAssignmentValue value) {
    Scope scope = new Scope(key.scope());
    if (!this.rootScope.containsScope(scope))
      return null;

    AccessPolicy accessPolicy = accessPolicy(key);
    if (accessPolicy == null)
      return null;

    // Add new assignment and access policies
    KafkaPrincipal principal = key.principal();
    RoleAssignmentValue oldValue = roleAssignments.put(key, value);
    NavigableMap<Resource, Set<AccessRule>> scopeRules =
        rbacAccessRules.computeIfAbsent(scope, s -> new ConcurrentSkipListMap<>());
    Map<Resource, Set<AccessRule>> rules = accessRules(key, value);
    rules.forEach((r, a) ->
        scopeRules.computeIfAbsent(r, x -> ConcurrentHashMap.newKeySet()).addAll(a));

    if (accessPolicy.isSuperUser())
      rbacSuperUsers.computeIfAbsent(scope, unused -> ConcurrentHashMap.newKeySet()).add(principal);

    // Remove access policy for any resources that were removed
    removeDeletedAccessPolicies(principal, scope);
    return oldValue;
  }

  private RoleAssignmentValue removeRoleAssignment(RoleAssignmentKey key) {
    Scope scope = new Scope(key.scope());
    if (!this.rootScope.containsScope(scope))
      return null;
    RoleAssignmentValue existing = roleAssignments.remove(key);
    if (existing != null) {
      removeDeletedAccessPolicies(key.principal(), scope);
      AccessPolicy accessPolicy = accessPolicy(key);
      if (accessPolicy != null && accessPolicy.isSuperUser()) {
        Set<KafkaPrincipal> superUsers = rbacSuperUsers.get(scope);
        if (superUsers != null)
          superUsers.remove(key.principal());
      }
      return existing;
    } else
      return null;
  }

  private UserValue updateUser(UserKey key, UserValue value) {
    UserMetadata oldValue = users.put(key.principal(), new UserMetadata(value.groups()));
    return oldValue == null ? null : new UserValue(oldValue.groups());
  }

  private void ensureNotFailed() {
    Map<Integer, String> exceptions = partitionStatus.entrySet().stream()
        .filter(e -> e.getValue().status() == MetadataStoreStatus.FAILED)
        .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().errorMessage()));
    if (!exceptions.isEmpty()) {
      throw new MetadataStoreException("Some partitions have failed: " + exceptions);
    }
  }

  private AccessPolicy accessPolicy(RoleAssignmentKey roleAssignmentKey) {
    Role role = rbacRoles.role(roleAssignmentKey.role());
    if (role == null) {
      log.error("Unknown role, ignoring role assignment {}", roleAssignmentKey);
      return null;
    } else {
      return role.accessPolicy();
    }
  }

  // Visibility for testing
  NavigableMap<Resource, Set<AccessRule>> rbacRules(Scope scope) {
    return rbacAccessRules.getOrDefault(scope, NO_RULES);
  }

  private Map<Resource, Set<AccessRule>> accessRules(RoleAssignmentKey roleAssignmentKey,
                                                     RoleAssignmentValue roleAssignmentValue) {
    Map<Resource, Set<AccessRule>> accessRules = new HashMap<>();
    KafkaPrincipal principal = roleAssignmentKey.principal();
    Collection<? extends Resource> resources;
    AccessPolicy accessPolicy = accessPolicy(roleAssignmentKey);
    if (accessPolicy != null) {
      if (roleAssignmentValue.resources().isEmpty()) {
        resources = accessPolicy.allowedOperations(ResourceType.CLUSTER).isEmpty() ?
            Collections.emptySet() : Collections.singleton(Resource.CLUSTER);

      } else {
        resources = roleAssignmentValue.resources();
      }
      for (Resource resource : resources) {
        Set<AccessRule> resourceRules = new HashSet<>();
        for (Operation op : accessPolicy.allowedOperations(resource.resourceType())) {
          AccessRule rule = new AccessRule(principal, PermissionType.ALLOW, WILDCARD_HOST, op,
              String.valueOf(roleAssignmentKey));
          resourceRules.add(rule);
        }
        accessRules.put(resource, resourceRules);
      }
    }
    return accessRules;
  }

  private void removeDeletedAccessPolicies(KafkaPrincipal principal, Scope scope) {
    NavigableMap<Resource, Set<AccessRule>> scopeRules = rbacRules(scope);
    if (scopeRules != null) {
      Map<Resource, Set<AccessRule>> deletedRules = new HashMap<>();
      scopeRules.forEach((resource, rules) -> {
        Set<AccessRule> principalRules = rules.stream()
            .filter(a -> a.principal().equals(principal))
            .collect(Collectors.toSet());
        deletedRules.put(resource, principalRules);
      });
      roleAssignments.entrySet().stream()
          .filter(e -> e.getKey().principal().equals(principal) && e.getKey().scope().equals(scope.name()))
          .flatMap(e -> accessRules(e.getKey(), e.getValue()).entrySet().stream())
          .forEach(e -> {
            Set<AccessRule> existing = deletedRules.get(e.getKey());
            if (existing != null)
              existing.removeAll(e.getValue());
          });
      deletedRules.forEach((resource, rules) -> {
        Set<AccessRule> resourceRules = scopeRules.get(resource);
        if (resourceRules != null) {
          resourceRules.removeAll(rules);
          if (resourceRules.isEmpty())
            scopeRules.remove(resource);
        }
      });
    }
  }
}