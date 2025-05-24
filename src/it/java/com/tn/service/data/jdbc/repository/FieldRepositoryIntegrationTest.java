package com.tn.service.data.jdbc.repository;

import static java.lang.String.format;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;
import java.util.stream.Stream;
import javax.sql.DataSource;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.jdbc.Sql;

import com.tn.service.data.jdbc.domain.Column;
import com.tn.service.data.jdbc.domain.Field;
import com.tn.service.data.jdbc.domain.FieldType;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
class FieldRepositoryIntegrationTest
{
  // Note: tests use H2 which is case-sensitive when getting metadata.
  private static final String SCHEMA = "PUBLIC";
  private static final String TABLE = "TEST";

  @Autowired
  DataSource dataSource;
  @Autowired
  FieldRepository fieldRepository;

  @ParameterizedTest
  @MethodSource({"types"})
  @Sql(executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD, statements = "DROP TABLE PUBLIC.TEST")
  void shouldFindFields(int columnType, FieldType fieldType) throws Exception
  {
    createTable(columnType);

    assertEquals(
      List.of(
        new Field("id", FieldType.INTEGER, new Column("ID", Types.INTEGER, true, false, false)),
        new Field("value1", fieldType, new Column("VALUE_1", columnType, false, false, false)),
        new Field("value2", fieldType, new Column("VALUE_2", columnType, false, true, false))
      ),
      fieldRepository.findForTable(SCHEMA, TABLE)
    );
  }

  static Stream<Arguments> types()
  {
    // Note: H2 doesn't support BIT or LONGNVARCHAR.
    return Stream.of(
      Arguments.of(Types.BOOLEAN, FieldType.BOOLEAN),
      Arguments.of(Types.TINYINT, FieldType.INTEGER),
      Arguments.of(Types.SMALLINT, FieldType.INTEGER),
      Arguments.of(Types.INTEGER, FieldType.INTEGER),
      Arguments.of(Types.BIGINT, FieldType.LONG),
      Arguments.of(Types.FLOAT, FieldType.FLOAT),
      Arguments.of(Types.DOUBLE, FieldType.DOUBLE),
      Arguments.of(Types.DECIMAL, FieldType.DECIMAL),
      Arguments.of(Types.CHAR, FieldType.TEXT),
      Arguments.of(Types.VARCHAR, FieldType.TEXT),
      Arguments.of(Types.DATE, FieldType.DATE),
      Arguments.of(Types.TIME, FieldType.TIME),
      Arguments.of(Types.TIMESTAMP, FieldType.TIMESTAMP)
    );
  }

  @Test
  @Sql(executionPhase = Sql.ExecutionPhase.AFTER_TEST_METHOD, statements = "DROP TABLE PUBLIC.TEST")
  void shouldFindFieldsWithAutoIncrement() throws Exception
  {
    createTable(Types.VARCHAR, true);

    assertEquals(
      List.of(
        new Field("id", FieldType.LONG, new Column("ID", Types.BIGINT, true, false, true)),
        new Field("value1", FieldType.TEXT, new Column("VALUE_1", Types.VARCHAR, false, false, false)),
        new Field("value2", FieldType.TEXT, new Column("VALUE_2", Types.VARCHAR, false, true, false))
      ),
      fieldRepository.findForTable(SCHEMA, TABLE)
    );
  }

  private void createTable(int type) throws SQLException, IllegalAccessException
  {
    createTable(type, false);
  }

  private void createTable(int type, boolean autoIncrement) throws SQLException, IllegalAccessException
  {
    try (
      Connection connection = dataSource.getConnection();
      Statement statement = connection.createStatement()
    )
    {
      String typeName = typeName(type);

      statement.executeUpdate(
        format(
          """
          CREATE TABLE %1$s.%2$s (
            id %3$s PRIMARY KEY,
            value_1 %4$s NOT NULL,
            value_2 %4$s NULL
          );
          """,
          SCHEMA,
          TABLE,
          autoIncrement ? "IDENTITY" : "INT NOT NULL",
          typeName
        )
      );
    }
  }

  private String typeName(int type) throws IllegalAccessException
  {
    for (var field : Types.class.getDeclaredFields())
    {
      if (field.getInt(null) == type) return field.getName();
    }

    throw new IllegalArgumentException("Unsupported data type: " + type);
  }
}
