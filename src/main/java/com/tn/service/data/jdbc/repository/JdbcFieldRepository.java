package com.tn.service.data.jdbc.repository;

import static com.tn.lang.Characters.UNDERSCORE;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import javax.sql.DataSource;

import com.tn.service.data.jdbc.domain.Column;
import com.tn.service.data.jdbc.domain.Field;
import com.tn.service.data.jdbc.domain.FieldType;
import com.tn.service.data.repository.FindException;

public class JdbcFieldRepository implements FieldRepository
{
  private static final String COLUMN_NAME = "COLUMN_NAME";
  private static final String DATA_TYPE = "DATA_TYPE";
  private static final String IS_AUTOINCREMENT = "IS_AUTOINCREMENT";
  private static final String IS_NULLABLE = "IS_NULLABLE";

  private final DataSource dataSource;

  public JdbcFieldRepository(DataSource dataSource)
  {
    this.dataSource = dataSource;
  }

  @Override
  public Collection<Field> findForTable(String schema, String table) throws FindException
  {
    try
    {
      return findAll(dataSource, schema, table);
    }
    catch (SQLException e)
    {
      throw new FindException(e);
    }
  }

  private Collection<Field> findAll(DataSource dataSource, String schema, String table) throws SQLException
  {
    try (Connection connection = dataSource.getConnection())
    {
      return fields(schema, table, connection.getMetaData());
    }
  }

  private static Collection<Field> fields(String schema, String table, DatabaseMetaData databaseMetaData) throws SQLException
  {
    Collection<String> keyColumnNames = keyColumnNames(schema, table, databaseMetaData);
    Collection<Field> fields = new ArrayList<>();

    try (ResultSet resultSet = databaseMetaData.getColumns(null, schema, table, null))
    {
      while (resultSet.next())
      {
        String columnName = resultSet.getString(COLUMN_NAME);
        int columnType = resultSet.getInt(DATA_TYPE);
        fields.add(
          new Field(
            toFieldName(columnName),
            toFieldType(columnType),
            new Column(
              columnName,
              columnType,
              keyColumnNames.contains(columnName),
              resultSet.getBoolean(IS_NULLABLE),
              resultSet.getBoolean(IS_AUTOINCREMENT)
            )
          )
        );
      }
    }

    return fields;
  }

  private static Set<String> keyColumnNames(String schema, String table, DatabaseMetaData databaseMetaData) throws SQLException
  {
    Set<String> keyColumnNames = new HashSet<>();

    try (ResultSet resultSet = databaseMetaData.getPrimaryKeys(null, schema, table))
    {
      while (resultSet.next()) keyColumnNames.add(resultSet.getString(COLUMN_NAME));
    }

    return keyColumnNames;
  }

  private static String toFieldName(String columnName)
  {
    StringBuilder fieldName = new StringBuilder();
    fieldName.append(Character.toLowerCase(columnName.charAt(0)));

    for (int i = 1; i < columnName.length(); i++)
    {
      char c = columnName.charAt(i);
      if (c == UNDERSCORE)
      {
        i++;
        if (i < columnName.length()) fieldName.append(Character.toUpperCase(columnName.charAt(i)));
      }
      else
      {
        fieldName.append(Character.toLowerCase(c));
      }
    }

    return fieldName.toString();
  }

  private static FieldType toFieldType(int dataType)
  {
    return switch (dataType)
    {
      case Types.BIT, Types.BOOLEAN -> FieldType.BOOLEAN;
      case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> FieldType.INTEGER;
      case Types.BIGINT -> FieldType.LONG;
      case Types.FLOAT -> FieldType.FLOAT;
      case Types.DOUBLE -> FieldType.DOUBLE;
      case Types.DECIMAL -> FieldType.DECIMAL;
      case Types.CHAR, Types.VARCHAR, Types.LONGNVARCHAR -> FieldType.TEXT;
      case Types.DATE -> FieldType.DATE;
      case Types.TIME -> FieldType.TIME;
      case Types.TIMESTAMP -> FieldType.TIMESTAMP;
      default -> throw new IllegalArgumentException("Unsupported data type: " + dataType);
    };
  }
}
