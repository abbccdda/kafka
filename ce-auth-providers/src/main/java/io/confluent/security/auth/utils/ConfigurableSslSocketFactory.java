// (Copyright) [2018 - 2019] Confluent, Inc.

package io.confluent.security.auth.utils;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.util.Map;
import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.common.security.ssl.SslFactory;

public class ConfigurableSslSocketFactory extends SSLSocketFactory {

  private static SslFactory sslFactory;

  private SSLSocketFactory sslSocketFactory;

  public static void createSslFactory(Map<String, ?> sslConfigs) {
    sslFactory = new SslFactory(Mode.CLIENT);
    sslFactory.configure(sslConfigs);
  }

  // This is not used directly, but is required for LDAP to create the SSL socket factory
  public static SocketFactory getDefault() {
    return new ConfigurableSslSocketFactory();
  }

  public ConfigurableSslSocketFactory() {
    sslSocketFactory = sslFactory.sslEngineBuilder().sslContext().getSocketFactory();
  }

  @Override
  public String[] getDefaultCipherSuites() {
    return sslSocketFactory.getDefaultCipherSuites();
  }

  @Override
  public String[] getSupportedCipherSuites() {
    return sslSocketFactory.getSupportedCipherSuites();
  }

  @Override
  public Socket createSocket(Socket socket, String host, int port, boolean autoClose)
      throws IOException {
    return sslSocketFactory.createSocket(socket, host, port, autoClose);
  }

  @Override
  public Socket createSocket() throws IOException {
    return sslSocketFactory.createSocket();
  }

  @Override
  public Socket createSocket(String host, int port) throws IOException {
    return sslSocketFactory.createSocket(host, port);
  }

  @Override
  public Socket createSocket(String host, int port, InetAddress localAddress, int localPort)
      throws IOException {
    return sslSocketFactory.createSocket(host, port, localAddress, localPort);
  }

  @Override
  public Socket createSocket(InetAddress address, int port) throws IOException {
    return sslSocketFactory.createSocket(address, port);
  }

  @Override
  public Socket createSocket(InetAddress address, int port, InetAddress remoteAddress,
      int remotePort) throws IOException {
    return sslSocketFactory.createSocket(address, port, remoteAddress, remotePort);
  }

  @Override
  public Socket createSocket(Socket socket, InputStream inputStream, boolean autoClose)
      throws IOException {
    return sslSocketFactory.createSocket(socket, inputStream, autoClose);
  }
}
