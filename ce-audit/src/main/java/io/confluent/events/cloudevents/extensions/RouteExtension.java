/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.events.cloudevents.extensions;

import io.cloudevents.extensions.ExtensionFormat;
import io.cloudevents.extensions.InMemoryFormat;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

public class RouteExtension {


  private String route;

  // Unmarshals the {@link RouteExtension} based on map of extensions.
  public static Optional<ExtensionFormat> unmarshall(Map<String, String> exts) {
    String route = exts.get(Format.ROUTE_KEY);
    if (route == null) {
      return Optional.empty();
    }

    RouteExtension r = new RouteExtension();
    r.setRoute(route);
    InMemoryFormat inMemory = InMemoryFormat.of(Format.IN_MEMORY_KEY, r, RouteExtension.class);
    return Optional
        .of(ExtensionFormat.of(inMemory, new AbstractMap.SimpleEntry<>(Format.ROUTE_KEY, route)));
  }

  public String getRoute() {
    return route;
  }

  public void setRoute(String route) {
    this.route = route;
  }

  // The in-memory format for routing.
  public static class Format implements ExtensionFormat {

    public static final String IN_MEMORY_KEY = "confluentRouting";
    public static final String ROUTE_KEY = "route";

    private final InMemoryFormat memory;
    private final Map<String, String> transport = new HashMap<>();

    public Format(RouteExtension extension) {
      Objects.requireNonNull(extension);

      memory = InMemoryFormat.of(IN_MEMORY_KEY, extension,
          RouteExtension.class);

      transport.put(ROUTE_KEY, extension.getRoute());
    }

    @Override
    public InMemoryFormat memory() {
      return memory;
    }

    @Override
    public Map<String, String> transport() {
      return transport;
    }
  }
}
