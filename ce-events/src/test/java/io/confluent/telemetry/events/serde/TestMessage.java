/*
 * Copyright [2020 - 2020] Confluent Inc.
 */
package io.confluent.telemetry.events.serde;

import com.google.common.collect.ImmutableList;
import java.util.List;

public class TestMessage {

  public static final TestMessage INSTANCE = new TestMessage(1, "bar", ImmutableList.of("F", "O", "O"));
  private int a;
  private String b;
  private List<String> c;

  public TestMessage() {
  }

  public TestMessage(int a, String b, List<String> c) {
    this.a = a;
    this.b = b;
    this.c = c;
  }

  public int getA() {
    return a;
  }

  public void setA(int a) {
    this.a = a;
  }

  public String getB() {
    return b;
  }

  public void setB(String b) {
    this.b = b;
  }

  public List<String> getC() {
    return c;
  }

  public void setC(List<String> c) {
    this.c = c;
  }
}
