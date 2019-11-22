package org.apache.kafka.common;

import java.lang.annotation.Documented;
import java.lang.annotation.Target;
import java.lang.annotation.ElementType;

@Documented
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface Confluent {
}
