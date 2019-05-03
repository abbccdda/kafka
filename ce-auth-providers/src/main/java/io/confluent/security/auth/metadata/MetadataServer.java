// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.auth.metadata;

import io.confluent.security.authorizer.Authorizer;
import java.io.Closeable;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.security.auth.AuthenticateCallbackHandler;

public interface MetadataServer extends Configurable, Closeable {

  /**
   * Starts metadata server. This method should return as soon as possible after initiating
   * server start since broker's startup is blocked until this method returns.
   * <p>
   * This method is invoked only after the reader associated with the  provided `authStore` is
   * initialized and the local cache is populated with existing entries. The metadata server
   * can use {@link AuthStore#writer()} to update the store.
   *
   * @param embeddedAuthorizer An embedded cross-component authorizer that can be used to authorize any action.
   * @param authStore An instance of AuthStore that can be used to read current role bindings,
   *                 user metadata etc. from an in-memory cache that is kept up-to-date by the embedded
   *                 authorizer.
   * @param authenticateCallbackHandler Authentication callback handlers, e.g. for LDAP authentication
   */
  void start(Authorizer embeddedAuthorizer,
             AuthStore authStore,
             AuthenticateCallbackHandler authenticateCallbackHandler);

  default String providerName() {
    return "RBAC";
  }
}
