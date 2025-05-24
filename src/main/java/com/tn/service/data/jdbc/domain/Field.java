package com.tn.service.data.jdbc.domain;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public record Field(String name, FieldType type, Column column)
{
  public <T> T getAsJavaType(ObjectNode object)
  {
    JsonNode value = getAsJsonType(object);

    try
    {
      return value != null ? type.asJavaType(value) : null;
    }
    catch (IllegalArgumentException e)
    {
      throw new IllegalArgumentException("Field " + name() + ", value " + value + " is not of type " + type());
    }
  }

  public JsonNode getAsJsonType(ObjectNode object)
  {
    JsonNode value = object.get(name());

    if (value == null || value.isNull())
    {
      if (column().nullable()) return null;
      else throw new IllegalStateException("Field " + name() + " does not allow nulls");
    }

    return value;
  }

  public <T> T getAsJavaType(ResultSet resultSet) throws SQLException
  {
    //noinspection unchecked
    return (T)type.get(resultSet, column().name());
  }

  public void set(ObjectNode object, Object value)
  {
    object.set(name(), type.asJsonType(value));
  }

  public <T extends JsonNode> T parseAsJsonType(String s)
  {
    return type.asJsonType(type.parse(s));
  }

  public boolean existsIn(ObjectNode object)
  {
    JsonNode value = object.get(name);
    return value != null && type.isJsonType(value);
  }

  public JsonNode coerce(JsonNode value)
  {
    return type.coerce(value);
  }
}
