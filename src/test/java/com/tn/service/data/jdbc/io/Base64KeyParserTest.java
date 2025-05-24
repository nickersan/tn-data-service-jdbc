package com.tn.service.data.jdbc.io;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import static com.tn.service.data.jdbc.domain.FieldType.BOOLEAN;
import static com.tn.service.data.jdbc.domain.FieldType.DATE;
import static com.tn.service.data.jdbc.domain.FieldType.DECIMAL;
import static com.tn.service.data.jdbc.domain.FieldType.DOUBLE;
import static com.tn.service.data.jdbc.domain.FieldType.FLOAT;
import static com.tn.service.data.jdbc.domain.FieldType.INTEGER;
import static com.tn.service.data.jdbc.domain.FieldType.LONG;
import static com.tn.service.data.jdbc.domain.FieldType.TEXT;
import static com.tn.service.data.jdbc.domain.FieldType.TIME;
import static com.tn.service.data.jdbc.domain.FieldType.TIMESTAMP;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.codec.binary.Base64;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import com.tn.service.data.io.InvalidKeyException;
import com.tn.service.data.jdbc.domain.Field;

class Base64KeyParserTest
{
  private static final Field BOOLEAN_ID = BOOLEAN.field("booleanId", null);
  private static final Field INTEGER_ID = INTEGER.field("integerId", null);
  private static final Field LONG_ID = LONG.field("longId", null);
  private static final Field FLOAT_ID = FLOAT.field("floatId", null);
  private static final Field DOUBLE_ID = DOUBLE.field("doubleId", null);
  private static final Field DECIMAL_ID = DECIMAL.field("decimalId", null);
  private static final Field TEXT_ID = TEXT.field("textId", null);
  private static final Field DATE_ID = DATE.field("dateId", null);
  private static final Field TIME_ID = TIME.field("timeId", null);
  private static final Field TIMESTAMP_ID = TIMESTAMP.field("timestampId", null);
  private static final Collection<Field> ALL_FIELDS = List.of(BOOLEAN_ID, INTEGER_ID, LONG_ID, FLOAT_ID, DOUBLE_ID, DECIMAL_ID, TEXT_ID, DATE_ID, TIME_ID, TIMESTAMP_ID);
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @ParameterizedTest
  @MethodSource("singleFieldKeys")
  void shouldParseSingleFieldKey(Field field, Object value)
  {
    assertEquals(key(field, value), new Base64KeyParser(List.of(field), OBJECT_MAPPER).parse(value.toString()));
  }

  private static Stream<Arguments> singleFieldKeys()
  {
    LocalDateTime now = LocalDateTime.now();

    Date date = Date.valueOf(now.toLocalDate());
    Time time = Time.valueOf(now.toLocalTime());

    return Stream.of(
      Arguments.of(BOOLEAN_ID, true),
      Arguments.of(INTEGER_ID, 12),
      Arguments.of(LONG_ID, 12L),
      Arguments.of(FLOAT_ID, 1.23F),
      Arguments.of(DOUBLE_ID, 1.23),
      Arguments.of(DECIMAL_ID, BigDecimal.valueOf(1.23)),
      Arguments.of(TEXT_ID, "ABC"),
      Arguments.of(DATE_ID, date),
      Arguments.of(TIME_ID, time),
      Arguments.of(TIMESTAMP_ID, now)
    );
  }

  @Test
  void shouldParseObjectKey() throws Exception
  {
    ObjectNode key = key();

    assertEquals(
      key,
      new Base64KeyParser(ALL_FIELDS, OBJECT_MAPPER).parse(Base64.encodeBase64String(OBJECT_MAPPER.writeValueAsBytes(key)))
    );
  }

  @ParameterizedTest
  @MethodSource("missingFields")
  void shouldThrowWhenKeyFieldMissing(Field missingField)
  {
    ObjectNode key = key();
    key.remove(missingField.name());

    assertThrows(
      InvalidKeyException.class,
      () ->new Base64KeyParser(ALL_FIELDS, OBJECT_MAPPER).parse(Base64.encodeBase64String(OBJECT_MAPPER.writeValueAsBytes(key)))
    );
  }

  static Stream<Arguments> missingFields()
  {
    return Stream.of(
      Arguments.of(BOOLEAN_ID),
      Arguments.of(INTEGER_ID),
      Arguments.of(LONG_ID),
      Arguments.of(FLOAT_ID),
      Arguments.of(DOUBLE_ID),
      Arguments.of(DECIMAL_ID),
      Arguments.of(TEXT_ID),
      Arguments.of(DATE_ID),
      Arguments.of(TIME_ID),
      Arguments.of(TIMESTAMP_ID)
    );
  }


  private static ObjectNode key()
  {
    LocalDateTime now = LocalDateTime.now();

    Date date = Date.valueOf(now.toLocalDate());
    Time time = Time.valueOf(now.toLocalTime());
    Timestamp timestamp = Timestamp.valueOf(now);

    ObjectNode key = new ObjectNode(null);
    BOOLEAN_ID.set(key, true);
    INTEGER_ID.set(key, 12);
    LONG_ID.set(key, 12L);
    FLOAT_ID.set(key, 1.23F);
    DOUBLE_ID.set(key, 1.23);
    DECIMAL_ID.set(key, BigDecimal.valueOf(1.23));
    TEXT_ID.set(key, "ABC");
    DATE_ID.set(key, date);
    TIME_ID.set(key, time);
    TIMESTAMP_ID.set(key, timestamp);

    return key;
  }

  private static Object key(Field field, Object value)
  {
    ObjectNode objectNode = new ObjectNode(null);
    field.set(objectNode, value instanceof LocalDateTime ? Timestamp.valueOf((LocalDateTime)value) : value);

    return objectNode;
  }
}
