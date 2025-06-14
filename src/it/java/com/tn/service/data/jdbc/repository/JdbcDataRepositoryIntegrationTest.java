package com.tn.service.data.jdbc.repository;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import static com.tn.lang.util.function.Lambdas.unwrapException;
import static com.tn.service.data.domain.Direction.DESCENDING;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.sql.DataSource;

import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.jdbc.Sql;

import com.tn.lang.util.Page;
import com.tn.lang.util.function.WrappedException;
import com.tn.service.data.jdbc.domain.Field;
import com.tn.service.data.jdbc.domain.FieldType;
import com.tn.service.data.repository.DataRepository;

@SpringBootTest(
  webEnvironment = SpringBootTest.WebEnvironment.NONE,
  properties =
  {
    "tn.data.schema=PUBLIC",
    "tn.data.table=TEST",
  }
)
@SuppressWarnings({"SqlNoDataSourceInspection", "SqlResolve", "SqlDeprecateType", "SpringBootApplicationProperties"})
class JdbcDataRepositoryIntegrationTest
{
  private static final String FIELD_ID = "id";
  private static final Field BOOLEAN_VALUE = FieldType.BOOLEAN.field("booleanValue", null);
  private static final Field INTEGER_VALUE = FieldType.INTEGER.field("integerValue", null);
  private static final Field LONG_VALUE = FieldType.LONG.field("longValue", null);
  private static final Field FLOAT_VALUE = FieldType.FLOAT.field("floatValue", null);
  private static final Field DOUBLE_VALUE = FieldType.DOUBLE.field("doubleValue", null);
  private static final Field DECIMAL_VALUE = FieldType.DECIMAL.field("decimalValue", null);
  private static final Field STRING_VALUE = FieldType.TEXT.field("stringValue", null);
  private static final Field DATE_VALUE = FieldType.DATE.field("dateValue", null);
  private static final Field TIME_VALUE = FieldType.TIME.field("timeValue", null);
  private static final Field TIMESTAMP_VALUE = FieldType.TIMESTAMP.field("timestampValue", null);

  @Autowired
  DataSource dataSource;
  @Autowired
  DataRepository<ObjectNode, ObjectNode> dataRepository;

  @AfterEach
  void deleteAll() throws SQLException
  {
    try (
      Connection connection = dataSource.getConnection();
      Statement statement = connection.createStatement()
    )
    {
      //noinspection SqlWithoutWhere,SqlResolve
      statement.executeUpdate("DELETE FROM PUBLIC.TEST");
    }
  }

  private static ObjectNode object(
    int id,
    Boolean booleanValue,
    int integerValue,
    long longValue,
    float floatValue,
    double doubleValue,
    BigDecimal decimalValue,
    String stringValue
  )
  {
    return object(
      id,
      booleanValue,
      integerValue,
      longValue,
      floatValue,
      doubleValue,
      decimalValue,
      stringValue,
      LocalDateTime.now()
    );
  }

  private static ObjectNode object(
    Integer id,
    Boolean booleanValue,
    int integerValue,
    long longValue,
    float floatValue,
    double doubleValue,
    BigDecimal decimalValue,
    String stringValue,
    LocalDateTime localDateTime
  )
  {
    Date dateValue = Date.valueOf(localDateTime.toLocalDate());
    Time timeValue = Time.valueOf(localDateTime.toLocalTime());
    Timestamp timestampValue = Timestamp.valueOf(localDateTime.withNano(0));

    ObjectNode objectNode = new ObjectNode(null);
    if (id != null) objectNode.set(FIELD_ID, IntNode.valueOf(id));
    if (booleanValue != null) BOOLEAN_VALUE.set(objectNode, booleanValue);
    INTEGER_VALUE.set(objectNode, integerValue);
    LONG_VALUE.set(objectNode, longValue);
    FLOAT_VALUE.set(objectNode, floatValue);
    DOUBLE_VALUE.set(objectNode, doubleValue);
    DECIMAL_VALUE.set(objectNode, decimalValue);
    STRING_VALUE.set(objectNode, stringValue);
    DATE_VALUE.set(objectNode, dateValue);
    TIME_VALUE.set(objectNode, timeValue);
    TIMESTAMP_VALUE.set(objectNode, timestampValue);

    return objectNode;
  }

  @Nested
  @DirtiesContext
  @Sql(
    executionPhase = Sql.ExecutionPhase.BEFORE_TEST_CLASS,
    statements = """
      CREATE TABLE PUBLIC.TEST (
        id              INT              NOT NULL PRIMARY KEY,
        boolean_value   BOOLEAN          NULL,
        integer_value   INTEGER          NOT NULL,
        long_value      LONG             NOT NULL,
        float_value     FLOAT            NOT NULL,
        double_value    DOUBLE PRECISION NOT NULL,
        decimal_value   DECIMAL(3, 2)    NOT NULL,
        string_value    VARCHAR(10)      NOT NULL,
        date_value      DATE             NOT NULL,
        time_value      TIME             NOT NULL,
        timestamp_value TIMESTAMP        NOT NULL
      );
    """
  )
  @Sql(
    executionPhase = Sql.ExecutionPhase.AFTER_TEST_CLASS,
    statements = "DROP TABLE PUBLIC.TEST"
  )
  class Find
  {
    @Test
    void shouldFind()
    {
      ObjectNode object1 = dataRepository.insert(object(1, true, 10, 11, 1.23F, 2.34, BigDecimal.valueOf(3.45), "T1"));
      ObjectNode object2 = dataRepository.insert(object(2, false, 11, 12, 2.23F, 3.34, BigDecimal.valueOf(4.45), "T2"));
      ObjectNode object3 = dataRepository.insert(object(3, true, 12, 13, 3.23F, 4.34, BigDecimal.valueOf(5.45), "T3"));

      assertEquals(object1, dataRepository.find(object1).orElse(null));
      assertEquals(object2, dataRepository.find(object2).orElse(null));
      assertEquals(object3, dataRepository.find(object3).orElse(null));
    }

    @Test
    void shouldNotFind()
    {
      ObjectNode key = new ObjectNode(null);
      key.set(FIELD_ID, IntNode.valueOf(1));

      assertTrue(dataRepository.find(key).isEmpty());
    }

    @Test
    void shouldFindAll()
    {
      ObjectNode object1 = dataRepository.insert(object(1, true, 10, 11, 1.23F, 2.34, BigDecimal.valueOf(3.45), "T1"));
      ObjectNode object2 = dataRepository.insert(object(2, false, 11, 12, 2.23F, 3.34, BigDecimal.valueOf(4.45), "T2"));
      ObjectNode object3 = dataRepository.insert(object(3, true, 12, 13, 3.23F, 4.34, BigDecimal.valueOf(5.45), "T3"));

      assertEquals(List.of(object1, object2, object3), dataRepository.findAll());
    }

    @Test
    void shouldFindAllWithSort()
    {
      ObjectNode object1 = dataRepository.insert(object(1, true, 10, 11, 1.23F, 2.34, BigDecimal.valueOf(3.45), "T1"));
      ObjectNode object2 = dataRepository.insert(object(2, false, 11, 12, 2.23F, 3.34, BigDecimal.valueOf(4.45), "T2"));
      ObjectNode object3 = dataRepository.insert(object(3, true, 12, 13, 3.23F, 4.34, BigDecimal.valueOf(5.45), "T3"));

      assertEquals(List.of(object3, object2, object1), dataRepository.findAll(Set.of("integerValue"), DESCENDING));
    }

    @Test
    void shouldFindAllPaginated()
    {
      ObjectNode object1 = dataRepository.insert(object(1, true, 10, 11, 1.23F, 2.34, BigDecimal.valueOf(3.45), "T1"));
      ObjectNode object2 = dataRepository.insert(object(2, false, 11, 12, 2.23F, 3.34, BigDecimal.valueOf(4.45), "T2"));
      ObjectNode object3 = dataRepository.insert(object(3, true, 12, 13, 3.23F, 4.34, BigDecimal.valueOf(5.45), "T3"));

      assertEquals(new Page<>(List.of(object1, object2), 0, 2, 3, 2), dataRepository.findAll(0, 2));
      assertEquals(new Page<>(List.of(object3), 1, 2, 3, 2), dataRepository.findAll(1, 2));
    }

    @Test
    void shouldFindAllPaginatedWithSort()
    {
      ObjectNode object1 = dataRepository.insert(object(1, true, 10, 11, 1.23F, 2.34, BigDecimal.valueOf(3.45), "T1"));
      ObjectNode object2 = dataRepository.insert(object(2, false, 11, 12, 2.23F, 3.34, BigDecimal.valueOf(4.45), "T2"));
      ObjectNode object3 = dataRepository.insert(object(3, true, 12, 13, 3.23F, 4.34, BigDecimal.valueOf(5.45), "T3"));

      assertEquals(new Page<>(List.of(object3, object2), 0, 2, 3, 2), dataRepository.findAll(0, 2, Set.of("integerValue"), DESCENDING));
      assertEquals(new Page<>(List.of(object1), 1, 2, 3, 2), dataRepository.findAll(1, 2, Set.of("integerValue"), DESCENDING));
    }

    @ParameterizedTest
    @MethodSource("findForObjectNodes")
    void shouldFindWhere(String query, ObjectNode objectNode, List<ObjectNode> objectNodes) throws Exception
    {
      try
      {
        dataRepository.insertAll(objectNodes);

        assertEquals(List.of(objectNode), dataRepository.findWhere(query));
      }
      catch (WrappedException e)
      {
        throw (Exception)unwrapException(e);
      }
    }

    @ParameterizedTest
    @MethodSource("findForObjectNodes")
    void shouldFindWhereWithSort(String query, ObjectNode objectNode, List<ObjectNode> objectNodes) throws Exception
    {
      try
      {
        dataRepository.insertAll(objectNodes);

        assertEquals(List.of(objectNode).reversed(), dataRepository.findWhere(query, Set.of("integerValue"), DESCENDING));
      }
      catch (WrappedException e)
      {
        throw (Exception)unwrapException(e);
      }
    }

    @ParameterizedTest
    @MethodSource("findForObjectNodes")
    void shouldFindWherePaginated(String query, ObjectNode objectNode, List<ObjectNode> objectNodes) throws Exception
    {
      try
      {
        dataRepository.insertAll(objectNodes);

        int pageNumber = 0;
        int pageSize = objectNodes.size();

        assertEquals(
          new Page<>(List.of(objectNode), pageNumber, pageSize, 1, 1),
          dataRepository.findWhere(query, pageNumber, pageSize)
        );
      }
      catch (WrappedException e)
      {
        throw (Exception)unwrapException(e);
      }
    }

    @ParameterizedTest
    @MethodSource("findForObjectNodes")
    void shouldFindWherePaginatedWithSort(String query, ObjectNode objectNode, List<ObjectNode> objectNodes) throws Exception
    {
      try
      {
        dataRepository.insertAll(objectNodes);

        int pageNumber = 0;
        int pageSize = objectNodes.size();

        assertEquals(
          new Page<>(List.of(objectNode).reversed(), pageNumber, pageSize, 1, 1),
          dataRepository.findWhere(query, pageNumber, pageSize, Set.of("integerValue"), DESCENDING)
        );
      }
      catch (WrappedException e)
      {
        throw (Exception)unwrapException(e);
      }
    }

    static Stream<Arguments> findForObjectNodes()
    {
      LocalDateTime now = LocalDateTime.now();

      ObjectNode object1 = object(1, true, 10, 11, 1.23F, 2.34, BigDecimal.valueOf(3.45), "T1", now.minusDays(1).minusMinutes(1));
      ObjectNode object2 = object(2, false, 11, 12, 2.23F, 3.34, BigDecimal.valueOf(4.45), "T2", now);
      ObjectNode object3 = object(3, null, 12, 13, 3.23F, 4.34, BigDecimal.valueOf(5.45), "T3", now.plusDays(1).plusMinutes(1));

      List<ObjectNode> objects = List.of(object1, object2, object3);

      return objects.stream()
        .flatMap(JdbcDataRepositoryIntegrationTest.Find::queryArguments)
        .peek(arguments -> arguments.add(objects))
        .map(arguments -> Arguments.of(arguments.toArray()));
    }

    private static Stream<List<Object>> queryArguments(ObjectNode objectNode)
    {
      return StreamSupport.stream(Spliterators.spliteratorUnknownSize(objectNode.fieldNames(), Spliterator.ORDERED), false)
        .map(fieldName -> fieldName + "=" + (objectNode.has(fieldName) ? objectNode.get(fieldName).asText() : "null"))
        .map(query -> new ArrayList<>(List.of(query, objectNode)));
    }
  }

  @Nested
  @DirtiesContext
  @Sql(
    executionPhase = Sql.ExecutionPhase.BEFORE_TEST_CLASS,
    statements = """
      CREATE TABLE PUBLIC.TEST (
        id              INT              NOT NULL PRIMARY KEY,
        boolean_value   BOOLEAN          NULL,
        integer_value   INTEGER          NOT NULL,
        long_value      LONG             NOT NULL,
        float_value     FLOAT            NOT NULL,
        double_value    DOUBLE PRECISION NOT NULL,
        decimal_value   DECIMAL(3, 2)    NOT NULL,
        string_value    VARCHAR(10)      NOT NULL,
        date_value      DATE             NOT NULL,
        time_value      TIME             NOT NULL,
        timestamp_value TIMESTAMP        NOT NULL
      );
    """
  )
  @Sql(
    executionPhase = Sql.ExecutionPhase.AFTER_TEST_CLASS,
    statements = "DROP TABLE PUBLIC.TEST"
  )
  class Insert
  {
    @Test
    void shouldInsert()
    {
      LocalDateTime now = LocalDateTime.now();

      ObjectNode object1 = object(1, true, 10, 11, 1.23F, 2.34, BigDecimal.valueOf(3.45), "T1", now.minusDays(1).minusMinutes(1));
      ObjectNode object2 = object(2, false, 11, 12, 2.23F, 3.34, BigDecimal.valueOf(4.45), "T2", now);
      ObjectNode object3 = object(3, null, 12, 13, 3.23F, 4.34, BigDecimal.valueOf(5.45), "T3", now.plusDays(1).plusMinutes(1));

      assertEquals(object1, dataRepository.insert(object1));
      assertEquals(object2, dataRepository.insert(object2));
      assertEquals(object3, dataRepository.insert(object3));
      assertEquals(List.of(object1, object2, object3), dataRepository.findAll());
    }

    @Test
    void shouldInsertInBatches()
    {
      LocalDateTime now = LocalDateTime.now();

      List<ObjectNode> objects = List.of(
        object(1, true, 10, 11, 1.23F, 2.34, BigDecimal.valueOf(3.45), "T1", now.minusDays(1).minusMinutes(1)),
        object(2, false, 11, 12, 2.23F, 3.34, BigDecimal.valueOf(4.45), "T2", now),
        object(3, null, 12, 13, 3.23F, 4.34, BigDecimal.valueOf(5.45), "T3", now.plusDays(1).plusMinutes(1))
      );

      DataRepository<ObjectNode, ObjectNode> dataRepository = ((JdbcDataRepository)JdbcDataRepositoryIntegrationTest.this.dataRepository).withBatchSize(2);

      assertEquals(objects, dataRepository.insertAll(objects));
      assertEquals(objects, dataRepository.findAll());
    }
  }

  @Nested
  @DirtiesContext
  @Sql(
    executionPhase = Sql.ExecutionPhase.BEFORE_TEST_CLASS,
    statements = """
      CREATE TABLE PUBLIC.TEST (
        id              IDENTITY         PRIMARY KEY,
        boolean_value   BOOLEAN          NULL,
        integer_value   INTEGER          NOT NULL,
        long_value      LONG             NOT NULL,
        float_value     FLOAT            NOT NULL,
        double_value    DOUBLE PRECISION NOT NULL,
        decimal_value   DECIMAL(3, 2)    NOT NULL,
        string_value    VARCHAR(10)      NOT NULL,
        date_value      DATE             NOT NULL,
        time_value      TIME             NOT NULL,
        timestamp_value TIMESTAMP        NOT NULL
      );
    """
  )
  @Sql(
    executionPhase = Sql.ExecutionPhase.AFTER_TEST_CLASS,
    statements = "DROP TABLE PUBLIC.TEST"
  )
  class InsertWithAutoIncrementId
  {
    private static final AtomicLong EXPECTED_ID = new AtomicLong(1);

    @Test
    void shouldInsert()
    {
      LocalDateTime now = LocalDateTime.now();

      ObjectNode object1 = object(null, true, 10, 11, 1.23F, 2.34, BigDecimal.valueOf(3.45), "T1", now.minusDays(1).minusMinutes(1));
      ObjectNode object2 = object(null, false, 11, 12, 2.23F, 3.34, BigDecimal.valueOf(4.45), "T2", now);
      ObjectNode object3 = object(null, null, 12, 13, 3.23F, 4.34, BigDecimal.valueOf(5.45), "T3", now.plusDays(1).plusMinutes(1));

      ObjectNode persistedObject1 = dataRepository.insert(object1);
      assertEquals(object1.set(FIELD_ID, LongNode.valueOf(EXPECTED_ID.getAndIncrement())), persistedObject1);

      ObjectNode persistedObject2 = dataRepository.insert(object2);
      assertEquals(object2.set(FIELD_ID, LongNode.valueOf(EXPECTED_ID.getAndIncrement())), persistedObject2);

      ObjectNode persistedObject3 = dataRepository.insert(object3);
      assertEquals(object3.set(FIELD_ID, LongNode.valueOf(EXPECTED_ID.getAndIncrement())), persistedObject3);

      assertEquals(List.of(persistedObject1, persistedObject2, persistedObject3), dataRepository.findAll());
    }

    @Test
    void shouldInsertInBatches()
    {
      LocalDateTime now = LocalDateTime.now();

      ObjectNode object1 = object(null, true, 10, 11, 1.23F, 2.34, BigDecimal.valueOf(3.45), "T1", now.minusDays(1).minusMinutes(1));
      ObjectNode object2 = object(null, false, 11, 12, 2.23F, 3.34, BigDecimal.valueOf(4.45), "T2", now);
      ObjectNode object3 = object(null, null, 12, 13, 3.23F, 4.34, BigDecimal.valueOf(5.45), "T3", now.plusDays(1).plusMinutes(1));

      DataRepository<ObjectNode, ObjectNode> dataRepository = ((JdbcDataRepository)JdbcDataRepositoryIntegrationTest.this.dataRepository).withBatchSize(2);

      List<ObjectNode> expectedObjects = List.of(
        object1.set(FIELD_ID, LongNode.valueOf(EXPECTED_ID.getAndIncrement())),
        object2.set(FIELD_ID, LongNode.valueOf(EXPECTED_ID.getAndIncrement())),
        object3.set(FIELD_ID, LongNode.valueOf(EXPECTED_ID.getAndIncrement()))
      );

      assertEquals(expectedObjects, dataRepository.insertAll(object1, object2, object3));
      assertEquals(expectedObjects, dataRepository.findAll());
    }
  }

  @Nested
  @DirtiesContext
  @Sql(
    executionPhase = Sql.ExecutionPhase.BEFORE_TEST_CLASS,
    statements = """
      CREATE TABLE PUBLIC.TEST (
        id              INT              NOT NULL PRIMARY KEY,
        boolean_value   BOOLEAN          NULL,
        integer_value   INTEGER          NOT NULL,
        long_value      LONG             NOT NULL,
        float_value     FLOAT            NOT NULL,
        double_value    DOUBLE PRECISION NOT NULL,
        decimal_value   DECIMAL(3, 2)    NOT NULL,
        string_value    VARCHAR(10)      NOT NULL,
        date_value      DATE             NOT NULL,
        time_value      TIME             NOT NULL,
        timestamp_value TIMESTAMP        NOT NULL
      );
    """
  )
  @Sql(
    executionPhase = Sql.ExecutionPhase.AFTER_TEST_CLASS,
    statements = "DROP TABLE PUBLIC.TEST"
  )
  class Update
  {
    @ParameterizedTest
    @MethodSource("updates")
    void shouldUpdate(ObjectNode object, ObjectNode mutation)
    {
      ObjectNode persisted = dataRepository.insert(object);

      ObjectNode mutated = new ObjectNode(null);
      mutated.setAll(persisted);
      mutated.setAll(mutation);

      assertEquals(mutated, dataRepository.update(mutation));
      assertEquals(mutated, dataRepository.find(mutation).orElse(null));
    }

    static Stream<Arguments> updates()
    {
      LocalDateTime now = LocalDateTime.now();
      ObjectNode object = object(1, true, 10, 11L, 1.23F, 2.34, BigDecimal.valueOf(3.45), "T1", now);

      return Stream.of(
        Arguments.of(object, mutation(object, Map.of(BOOLEAN_VALUE, false))),
        Arguments.of(object, mutation(object, Map.of(INTEGER_VALUE, 11))),
        Arguments.of(object, mutation(object, Map.of(LONG_VALUE, 12L))),
        Arguments.of(object, mutation(object, Map.of(FLOAT_VALUE, 2.34F))),
        Arguments.of(object, mutation(object, Map.of(DOUBLE_VALUE, 3.45))),
        Arguments.of(object, mutation(object, Map.of(DECIMAL_VALUE, BigDecimal.valueOf(4.56)))),
        Arguments.of(object, mutation(object, Map.of(STRING_VALUE, "U2"))),
        Arguments.of(object, mutation(object, Map.of(DATE_VALUE, Date.valueOf(now.plusDays(1).toLocalDate())))),
        Arguments.of(object, mutation(object, Map.of(TIME_VALUE, Time.valueOf(now.plusMinutes(1).toLocalTime())))),
        Arguments.of(object, mutation(object, Map.of(TIMESTAMP_VALUE, Timestamp.valueOf(now.plusSeconds(1).withNano(0))))),
        Arguments.of(
          object,
          mutation(
            object,
            Map.of(
              BOOLEAN_VALUE, false,
              INTEGER_VALUE, 11,
              LONG_VALUE, 12L,
              FLOAT_VALUE, 2.34F,
              DOUBLE_VALUE, 3.45,
              DECIMAL_VALUE, BigDecimal.valueOf(4.56),
              STRING_VALUE, "U2",
              DATE_VALUE, Date.valueOf(now.plusDays(1).toLocalDate()),
              TIME_VALUE, Time.valueOf(now.plusMinutes(1).toLocalTime()),
              TIMESTAMP_VALUE, Timestamp.valueOf(now.plusSeconds(1).withNano(0))
            )
          )
        )
      );
    }

    @Test
    void shouldUpdateAll()
    {
      LocalDateTime now = LocalDateTime.now();

      ObjectNode object1 = dataRepository.insert(object(1, true, 10, 11, 1.23F, 2.34, BigDecimal.valueOf(3.45), "T1", now.minusDays(1).minusMinutes(1)));
      ObjectNode object2 = dataRepository.insert(object(2, false, 11, 12, 2.23F, 3.34, BigDecimal.valueOf(4.45), "T2", now));
      ObjectNode object3 = dataRepository.insert(object(3, null, 12, 13, 3.23F, 4.34, BigDecimal.valueOf(5.45), "T3", now.plusDays(1).plusMinutes(1)));

      DataRepository<ObjectNode, ObjectNode> dataRepository = ((JdbcDataRepository)JdbcDataRepositoryIntegrationTest.this.dataRepository).withBatchSize(2);

      ObjectNode mutation1 = new ObjectNode(null);
      mutation1.set(FIELD_ID, object1.get(FIELD_ID));
      mutation1.set("booleanValue", BooleanNode.valueOf(false));

      ObjectNode mutation2 = new ObjectNode(null);
      mutation2.set(FIELD_ID, object2.get(FIELD_ID));
      mutation2.set("booleanValue", BooleanNode.valueOf(true));

      ObjectNode mutation3 = new ObjectNode(null);
      mutation3.set(FIELD_ID, object3.get(FIELD_ID));
      mutation3.set("booleanValue", BooleanNode.valueOf(false));

      ObjectNode mutated1 = new ObjectNode(null);
      mutated1.setAll(object1);
      mutated1.setAll(mutation1);

      ObjectNode mutated2 = new ObjectNode(null);
      mutated2.setAll(object2);
      mutated2.setAll(mutation2);

      ObjectNode mutated3 = new ObjectNode(null);
      mutated3.setAll(object3);
      mutated3.setAll(mutation3);

      assertEquals(List.of(mutated1, mutated2, mutated3), dataRepository.updateAll(mutation1, mutation2, mutation3));
      assertEquals(List.of(mutated1, mutated2, mutated3), dataRepository.findAll());
    }

    private static Object mutation(ObjectNode object, Map<Field, Object> values)
    {
      ObjectNode mutation = new ObjectNode(null);
      mutation.set(FIELD_ID, object.get(FIELD_ID));
      values.forEach((field, value) -> field.set(mutation, value));

      return mutation;
    }
  }

  @Nested
  @DirtiesContext
  @Sql(
    executionPhase = Sql.ExecutionPhase.BEFORE_TEST_CLASS,
    statements = """
      CREATE TABLE PUBLIC.TEST (
        id              INT              NOT NULL PRIMARY KEY,
        boolean_value   BOOLEAN          NULL,
        integer_value   INTEGER          NOT NULL,
        long_value      LONG             NOT NULL,
        float_value     FLOAT            NOT NULL,
        double_value    DOUBLE PRECISION NOT NULL,
        decimal_value   DECIMAL(3, 2)    NOT NULL,
        string_value    VARCHAR(10)      NOT NULL,
        date_value      DATE             NOT NULL,
        time_value      TIME             NOT NULL,
        timestamp_value TIMESTAMP        NOT NULL
      );
    """
  )
  @Sql(
    executionPhase = Sql.ExecutionPhase.AFTER_TEST_CLASS,
    statements = "DROP TABLE PUBLIC.TEST"
  )
  class Delete
  {
    @Test
    void shouldDelete()
    {
      ObjectNode object1 = dataRepository.insert(object(1, true, 10, 11, 1.23F, 2.34, BigDecimal.valueOf(3.45), "T1"));
      ObjectNode object2 = dataRepository.insert(object(2, false, 11, 12, 2.23F, 3.34, BigDecimal.valueOf(4.45), "T2"));
      ObjectNode object3 = dataRepository.insert(object(3, null, 12, 13, 3.23F, 4.34, BigDecimal.valueOf(5.45), "T3"));

      assertEquals(List.of(object1, object2, object3), dataRepository.findAll());
      dataRepository.delete(object2);
      assertEquals(List.of(object1, object3), dataRepository.findAll());
      dataRepository.delete(object1);
      assertEquals(List.of(object3), dataRepository.findAll());
      dataRepository.delete(object3);
      assertTrue(dataRepository.findAll().isEmpty());
    }

    @Test
    void shouldDeleteAll()
    {
      ObjectNode object1 = dataRepository.insert(object(1, true, 10, 11, 1.23F, 2.34, BigDecimal.valueOf(3.45), "T1"));
      ObjectNode object2 = dataRepository.insert(object(2, false, 11, 12, 2.23F, 3.34, BigDecimal.valueOf(4.45), "T2"));
      ObjectNode object3 = dataRepository.insert(object(3, null, 12, 13, 3.23F, 4.34, BigDecimal.valueOf(5.45), "T3"));

      assertEquals(List.of(object1, object2, object3), dataRepository.findAll());
      dataRepository.deleteAll(object1, object2);
      assertEquals(List.of(object3), dataRepository.findAll());
      dataRepository.deleteAll(object3);
      assertTrue(dataRepository.findAll().isEmpty());
    }
  }
}
