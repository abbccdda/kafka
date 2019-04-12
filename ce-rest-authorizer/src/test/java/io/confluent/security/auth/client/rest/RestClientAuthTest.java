// (Copyright) [2019 - 2019] Confluent, Inc.

package io.confluent.security.auth.client.rest;

import io.confluent.security.auth.client.RestClientConfig;
import io.confluent.security.auth.client.provider.BuiltInAuthProviders;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.verify;
import static org.powermock.api.easymock.PowerMock.expectNew;
import static org.powermock.api.easymock.PowerMock.replay;


@RunWith(PowerMockRunner.class)
@PrepareForTest(RestClient.class)
@Ignore
public class RestClientAuthTest {

  @Mock
  private URL url;

  @Test
  public void testSetBasicAuthRequestHeader() throws Exception {
    HttpURLConnection httpURLConnection = createNiceMock(HttpURLConnection.class);
    InputStream inputStream = createNiceMock(InputStream.class);

    expectNew(URL.class, anyString()).andReturn(url).times(2);
    expect(url.getProtocol()).andReturn("http");
    expect(url.openConnection()).andReturn(httpURLConnection);
    expect(httpURLConnection.getResponseCode()).andReturn(HttpURLConnection.HTTP_OK);

    // Make sure that the Authorization header is set with the correct value for "user:password"
    httpURLConnection.setRequestProperty("Authorization", "Basic dXNlcjpwYXNzd29yZA==");
    expectLastCall().once();

    expect(httpURLConnection.getInputStream()).andReturn(inputStream);

    expect(inputStream.read((byte[]) anyObject(), anyInt(), anyInt()))
        .andDelegateTo(new InputStream() {
          @Override
          public int read() {
            return 0;
          }

          @Override
          public int read(byte[] b, int off, int len) {
            byte[] json = "[\"abc\"]".getBytes(StandardCharsets.UTF_8);
            System.arraycopy(json, 0, b, 0, json.length);
            return json.length;
          }
        }).anyTimes();

    replay(URL.class, url);
    replay(HttpURLConnection.class, httpURLConnection);
    replay(InputStream.class, inputStream);

    Map<String, Object> configs = new HashMap<>();
    configs.put(RestClientConfig.BOOTSTRAP_METADATA_SERVER_URLS_PROP, "http://localhost:8080");
    configs.put(RestClientConfig.BASIC_AUTH_CREDENTIALS_PROVIDER_PROP, BuiltInAuthProviders.BasicAuthCredentialProviders.USER_INFO.name());
    configs.put(RestClientConfig.BASIC_AUTH_USER_INFO_PROP, "user:password");
    new RestClient(configs);

    verify(httpURLConnection);
  }
}
