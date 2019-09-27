// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.client.acl;

import com.fasterxml.jackson.core.type.TypeReference;
import io.confluent.security.auth.client.rest.RestClient;
import io.confluent.security.auth.client.rest.RestRequest;
import io.confluent.security.auth.client.rest.entities.AclFilter;
import io.confluent.security.auth.client.rest.entities.CreateAclsRequest;
import io.confluent.security.auth.client.rest.entities.CreateAclsResult;
import io.confluent.security.auth.client.rest.entities.DeleteAclsRequest;
import io.confluent.security.auth.client.rest.entities.DeleteAclsResult;
import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.acl.AclBinding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

public class MdsAclClient implements Configurable, AutoCloseable {

  private static final Logger log = LoggerFactory.getLogger(MdsAclClient.class);

  private static final String ACL_CREATE_ENDPOINT = "/acls/create";
  private static final String ACL_DELETE_ENDPOINT = "/acls/delete";
  private static final String ACL_LIST_ENDPOINT = "/acls:search";

  private static final TypeReference<CreateAclsResult> CREATE_RESPONSE_TYPE =
      new TypeReference<CreateAclsResult>() { };
  private static final TypeReference<DeleteAclsResult> DELETE_RESPONSE_TYPE =
      new TypeReference<DeleteAclsResult>() { };
  private static final TypeReference<Collection<AclBinding>> LIST_RESPONSE_TYPE =
      new TypeReference<Collection<AclBinding>>() { };

  private RestClient restClient;

  public MdsAclClient() { }

  @Override
  public void configure(final Map<String, ?> configs) {
    if (this.restClient != null) {
      log.warn("Using the existing RestClient instance");
      return;
    }
    this.restClient = new RestClient(configs);
  }

  public CreateAclsResult createAcls(final CreateAclsRequest createAclRequest) {
    RestRequest restRequest = createRequest(ACL_CREATE_ENDPOINT,
        "POST",
        createAclRequest,
        CREATE_RESPONSE_TYPE);
    return doRequest(restRequest);
  }

  public DeleteAclsResult deleteAcls(final DeleteAclsRequest deleteAclRequest) {
    RestRequest restRequest = createRequest(ACL_DELETE_ENDPOINT,
        "DELETE",
        deleteAclRequest,
        DELETE_RESPONSE_TYPE);
    return doRequest(restRequest);
  }

  public Collection<AclBinding> describeAcls(final AclFilter describeAclRequest) {
    RestRequest restRequest = createRequest(ACL_LIST_ENDPOINT,
        "POST",
        describeAclRequest,
        LIST_RESPONSE_TYPE);
    return doRequest(restRequest);
  }

  private RestRequest createRequest(final String path,
                                    final String requestMethod,
                                    final Object payload,
                                    final TypeReference<?> responseType) {

    if (restClient == null)
      throw new IllegalStateException("RestClient has not been initialized.");

    RestRequest request = this.restClient.newRequest(path);
    request.setRequest(payload);
    request.setRequestMethod(requestMethod);
    request.setResponse(responseType);
    return request;
  }

  private <T> T doRequest(RestRequest request) {
    try {
      return restClient.sendRequest(request);
    } catch (Exception e) {
      log.error("Error occurred while executing Acl request {}", request, e);
      throw new RuntimeException("Error occurred while executing Acl request: " + e.getMessage(), e);
    }
  }

  @Override
  public void close() {
    if (restClient != null)
      restClient.close();
  }
}
