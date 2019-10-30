/*
 * Copyright [2019 - 2019] Confluent Inc.
 */
package io.confluent.crn;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A ConfluentResourceName is a compound identifier that identifies a resource or pattern.
 *
 * The textual representation of a CRN is a URI with a scheme of "crn". The Authority section of the
 * URI SHOULD BE a domain name controlled by the organization that is responsible for the names. The
 * Path section of the URI is a sequence of type=identifier elements, separated by /. The
 * identifiers are URL Encoded, so that any unusual characters are escaped.
 *
 * If the CRN represents a pattern, it will contain a literal '*' (not URL Encoded) at the end of an
 * identifier for a PREFIX pattern, or in place of an identifier for an ANY pattern.
 */
public class ConfluentResourceName implements Comparable {

  private static final Logger log = LoggerFactory.getLogger(ConfluentResourceName.class);

  private static final String SCHEME = "crn";
  private static final String SCHEME_PART = SCHEME + "://";
  private static final String PATH_DELIMITER = "/";
  private static final String ELEMENT_JOINER = "=";
  private static final String WILDCARD_CHARACTER = "*";

  public static final String DEFAULT_AUTHORITY = "";

  private String authority;
  private List<Element> nameElements;

  private String stringForm;

  public static class Element implements Comparable {

    private String resourceType;
    private String encodedResourceName;

    public Element(String resourceType, String encodedResourceName) throws CrnSyntaxException {
      if (resourceType.contains(ELEMENT_JOINER) || resourceType.contains(PATH_DELIMITER)) {
        throw new CrnSyntaxException(resourceType,
            String.format("resourceType cannot contain '%s' or '%s'",
                PATH_DELIMITER, ELEMENT_JOINER));
      }
      this.resourceType = resourceType;
      this.encodedResourceName = encodedResourceName;
    }

    public String encodedResourceName() {
      return encodedResourceName;
    }

    public String resourceType() {
      return resourceType;
    }

    private boolean isPrefix() {
      return encodedResourceName.endsWith(WILDCARD_CHARACTER);
    }

    private String resourceNameWithoutWildcard() {
      if (isPrefix()) {
        return encodedResourceName.substring(0, encodedResourceName.length() - 1);
      }
      return encodedResourceName;
    }

    public boolean matches(Element that) {
      if (!this.resourceType.equals(that.resourceType)) {
        return false;
      }
      if (isPrefix()) {
        return that.encodedResourceName.startsWith(resourceNameWithoutWildcard());
      }
      return this.encodedResourceName.equals(that.encodedResourceName);
    }

    @Override
    public String toString() {
      return resourceType + ELEMENT_JOINER + encodedResourceName;
    }

    @Override
    public int compareTo(Object o) {
      Element that = (Element) o;
      if (!resourceType.equals(that.resourceType)) {
        return resourceType.compareTo(that.resourceType);
      }
      int match = resourceNameWithoutWildcard().compareTo(that.resourceNameWithoutWildcard());
      if (match == 0) {
        if (this.isPrefix()) {
          if (that.isPrefix()) {
            return 0;
          }
          return -1; // prefixed element comes first
        }
        if (that.isPrefix()) {
          return 1; // prefixed element comes first
        }
        return 0;
      }
      return match;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      Element that = (Element) o;
      return Objects.equals(this.resourceType, that.resourceType) &&
          Objects.equals(this.encodedResourceName, that.encodedResourceName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(resourceType, encodedResourceName);
    }

  }

  /**
   * Produce a CRN from the given authority with the given elements
   */
  private ConfluentResourceName(String authority, List<Element> elements) {
    this(authority, elements, null);
  }

  /**
   * Produce a CRN from the given authority with the given elements
   */
  private ConfluentResourceName(String authority, List<Element> elements, String stringForm) {
    this.authority = authority == null || authority.isEmpty() ? DEFAULT_AUTHORITY : authority;
    this.nameElements = Collections.unmodifiableList(elements);
    this.stringForm = stringForm;
  }

  /**
   * Return the authority of this CRN
   */
  public String authority() {
    return authority;
  }

  /**
   * Return an ordered list of the elements of this CRN
   */
  public List<Element> elements() {
    return Collections.unmodifiableList(nameElements);
  }

  /**
   * Returns the last element in the list of elements. Since the list is ordered
   * general-to-specific, this is the resource represented by the name without its scope.
   *
   * Because the Builder enforces that there's at least one element, this should always return
   * something.
   */
  public Element lastResourceElement() {
    List<Element> elements = elements();
    if (elements.isEmpty()) {
      return null;
    }
    return elements.get(elements.size() - 1);
  }

  /**
   * Returns the Resource Type of the resource(s) referred to by the CRN. This is
   * the Resource Type of the last element.
   * @return
   */
  public String resourceType() {
    return lastResourceElement().resourceType();
  }

  /**
   * Treating this CRN as a pattern, does the other CRN match it?
   */
  public boolean matches(ConfluentResourceName that) {
    if (!this.authority.equals(that.authority)) {
      return false;
    }
    if (this.elements().size() != that.elements().size()) {
      return false;
    }
    for (int i = this.elements().size() - 1; i >= 0; i--) {
      if (!this.elements().get(i).matches(that.elements().get(i))) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String toString() {
    if (stringForm == null) {
      stringForm = SCHEME + "://" + authority + "/" +
          nameElements.stream().map(Element::toString).collect(Collectors.joining(PATH_DELIMITER));
    }
    return stringForm;
  }

  @Override
  public int compareTo(Object o) {
    ConfluentResourceName that = (ConfluentResourceName) o;
    if (!this.authority.equals(that.authority)) {
      return this.authority.compareTo(that.authority);
    }
    int shorter = Math.min(this.elements().size(), that.elements().size());
    for (int i = 0; i < shorter; i++) {
      int elementComparison = this.elements().get(i).compareTo(that.elements().get(i));
      if (elementComparison != 0) {
        return elementComparison;
      }
    }
    return this.elements().size() - that.elements().size();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ConfluentResourceName that = (ConfluentResourceName) o;
    return Objects.equals(this.authority, that.authority) &&
        Objects.equals(this.nameElements, that.nameElements);
  }

  @Override
  public int hashCode() {
    return Objects.hash(authority, nameElements);
  }

  /**
   * Return a CRN of the same form as this one, with all resource names as wildcards
   */
  public ConfluentResourceName allWildcards() {
    Builder builder = newBuilder();
    builder.setAuthority(authority);
    try {
      for (Element e : nameElements) {
        builder.addElementWithWildcard(e.resourceType, "");
      }
      return builder.build();
    } catch (CrnSyntaxException ex) {
      // This should be impossible
      throw new RuntimeException(ex);
    }
  }

  /**
   * Parse the given string to a ConfluentResourceName, throw if it can't be parsed
   */
  public static ConfluentResourceName fromString(String crn) throws CrnSyntaxException {
    if (!crn.startsWith(SCHEME_PART)) {
      throw new CrnSyntaxException(crn, "Scheme is not " + SCHEME);
    }
    String rest = crn.substring(SCHEME_PART.length());
    String[] parts = rest.split(PATH_DELIMITER);
    Builder builder = ConfluentResourceName.newBuilder();
    builder.setStringForm(crn);
    builder.setAuthority(parts[0]);

    final List<CrnSyntaxException> exceptions = new ArrayList<>();
    List<Element> elements = Arrays.stream(parts).skip(1).flatMap(
        element -> {
          String[] split = element.split(ELEMENT_JOINER);
          if (split.length != 2) {
            exceptions.add(new CrnSyntaxException(crn, "Invalid element: " + element));
            return Stream.empty();
          }
          try {
            return Stream.of(new Element(split[0], split[1]));
          } catch (CrnSyntaxException e) {
            exceptions.add(e);
            return Stream.empty();
          }
        }).collect(Collectors.toList());
    if (!exceptions.isEmpty()) {
      if (exceptions.size() == 1) {
        throw exceptions.get(0);
      }
      throw new CrnSyntaxException(crn, exceptions);
    }
    builder.addAllElements(elements);
    return builder.build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static final class Builder {

    private String authority = null;
    private ArrayList<Element> elements = new ArrayList<>();
    private String stringForm = null;

    private Builder() {
      // Call newBuilder instead
    }

    public Builder setAuthority(String authority) {
      this.authority = authority;
      return this;
    }

    private void setStringForm(String stringForm) {
      this.stringForm = stringForm;
    }

    /**
     * This encodes the resource name.
     *
     * We do RFC 1738 URL Encoding, with the additional character "*" being encoded as "%2A" to
     * distinguish it from wildcards
     */
    public Builder addElement(String resourceType, String unencodedResourceName)
        throws CrnSyntaxException {
      try {
        String encodedResourceName =
            URLEncoder.encode(unencodedResourceName, StandardCharsets.UTF_8.name())
                .replace(WILDCARD_CHARACTER, "%2A");
        elements.add(new Element(resourceType, encodedResourceName));
        return this;
      } catch (UnsupportedEncodingException e) {
        // This should never happen, since we're using the predefined charset
        throw new RuntimeException(e);
      }
    }

    /**
     * This URLEncodes the resource name and then adds a wildcard marker after it.
     *
     * We do RFC 1738 URL Encoding, with the additional character "*" being encoded as "%2A" to
     * distinguish it from wildcards
     */
    public Builder addElementWithWildcard(String resourceType, String unencodedResourceName)
        throws CrnSyntaxException {
      try {
        String encodedResourceName =
            URLEncoder.encode(unencodedResourceName, StandardCharsets.UTF_8.name())
                .replace(WILDCARD_CHARACTER, "%2A")
                + WILDCARD_CHARACTER;
        elements.add(new Element(resourceType, encodedResourceName));
        return this;
      } catch (UnsupportedEncodingException e) {
        // This should never happen, since we're using the predefined charset
        throw new RuntimeException(e);
      }
    }

    /**
     * The caller is responsible for ensuring that elements are properly encoded
     */
    public Builder addAllElements(List<Element> elements) {
      this.elements.addAll(elements);
      return this;
    }

    public ConfluentResourceName build() throws CrnSyntaxException {
      if (elements.isEmpty()) {
        throw new CrnSyntaxException("", "Need at least one Element");
      }
      return new ConfluentResourceName(this.authority, this.elements, this.stringForm);
    }
  }
}
