// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.client.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.confluent.security.auth.client.provider.HttpCredentialProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URI;
import java.net.URL;

public class RestRequest {
  private static final Logger log = LoggerFactory.getLogger(RestRequest.class);

  private static ObjectMapper objectMapper = new ObjectMapper();

  static final String API_ENDPOINT = "/security/1.0";

  private UriBuilder builder;
  private HttpCredentialProvider credentialProvider;
  private TypeReference responseReference;

  private String method = "GET";
  private Object request;


  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  RestRequest(String protocol, String path) {
    this.builder = UriBuilder
            .fromPath(API_ENDPOINT)
            .path(path)
            .scheme(protocol);
  }

  /*
   * Configures connection object with any additional context required to fulfill this request
   * Initially this will just be used to set authentication headers if configured.
   */
  void configureConnection(HttpURLConnection connection)
          throws ProtocolException {

    if (this.credentialProvider != null) {
      connection.setRequestProperty("Authorization",
              String.join(" ",
                      credentialProvider.getScheme(),
                      credentialProvider.getCredentials()));
    }

    if (this.request != null) {
      connection.setDoOutput(true);
    }

    /* connection.getResponseCode() implicitly calls getInputStream, so always set to true. */
    connection.setDoInput(true);
    connection.setRequestMethod(method);
  }

  /* Returns a new URI derived from the current state of this request's builder object. */
  URI build(String host) {
    return this.builder.host(host).build();
  }

  URL build() throws MalformedURLException {
    return this.builder.build().toURL();
  }

  /*
   * Writes the request body contents to the output stream if present.
   * It is the caller's responsibility to close the output stream.
   */
  void writeRequestBody(OutputStream out) throws IOException {
    if (this.request != null) {
      OBJECT_MAPPER.writeValue(out, this.request);
    }
  }

  <T> T readResponse(InputStream in) throws IOException {
    if (this.responseReference != null) {
      return objectMapper.readValue(in, this.responseReference);
    }
    return null;
  }

  void setHost(String host) {
    this.builder.host(host);
  }

  void setPort(int port) {
    this.builder.port(port);
  }

  /* Configures the credential provider to be used when making requests */
  public void setCredentialProvider(HttpCredentialProvider credentialProvider) {
    this.credentialProvider = credentialProvider;
  }

  /* Sets request methood [GET, PUT, POST, etc] */
  public void setRequestMethod(String method) {
    this.method = method;
  }

  /* Appends the provided segment to the existing path */
  public void addPath(String path) {
    this.builder.path(path);
  }

  /* Resets the path to the base then appends the provided segment */
  public void setPath(String path) {
    this.builder
            .replacePath(API_ENDPOINT)
            .path(path);
  }

  /* Appends a query parameter to the existing query string */
  public void addQueryParam(String name, Object... params) {
    this.builder.queryParam(name, params);
  }

  /*
   * Sets request body
   * All request body objects will be serialized as JSON
   * It is recommended you take advantage of @JsonCreator annotations
   */
  public void setRequest(Object body) {
    this.request = body;
  }

  public void setResponse(final TypeReference responseFormat) {
    this.responseReference = responseFormat;
  }
}
