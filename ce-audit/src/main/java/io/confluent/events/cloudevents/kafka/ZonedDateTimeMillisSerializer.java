package io.confluent.events.cloudevents.kafka;

import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;

/**
 * This Serializer always includes exactly 3 digits after the decimal point, capturing
 * milliseconds. Trailing 0s are included.
 */
public class ZonedDateTimeMillisSerializer extends StdSerializer<ZonedDateTime> {

  DateTimeFormatter formatter = new DateTimeFormatterBuilder()
      .append(DateTimeFormatter.ISO_LOCAL_DATE)
      .appendLiteral("T")
      .appendValue(HOUR_OF_DAY, 2)
      .appendLiteral(':')
      .appendValue(MINUTE_OF_HOUR, 2)
      .optionalStart()
      .appendLiteral(':')
      .appendValue(SECOND_OF_MINUTE, 2)
      .appendFraction(ChronoField.MICRO_OF_SECOND, 3, 3, true)
      .appendOffsetId()
      .toFormatter();

  public ZonedDateTimeMillisSerializer() {
    this(null, false);
  }

  protected ZonedDateTimeMillisSerializer(Class<?> t, boolean dummy) {
    super(t, dummy);
  }

  @Override
  public void serialize(ZonedDateTime time, JsonGenerator generator,
      SerializerProvider provider) throws IOException {

    generator.writeString(time.format(formatter));

  }

}
