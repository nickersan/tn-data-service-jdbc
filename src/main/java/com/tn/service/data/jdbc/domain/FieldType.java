package com.tn.service.data.jdbc.domain;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DecimalNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.TextNode;

public enum FieldType
{
  BOOLEAN(Boolean.class)
  {
    @Override
    public boolean isJsonType(JsonNode value)
    {
      return value != null && value.isBoolean();
    }

    @Override
    protected Object get(ResultSet resultSet, String columnName) throws SQLException
    {
      boolean value = resultSet.getBoolean(columnName);
      return resultSet.wasNull() ? null : value;
    }

    @Override
    protected Object castJavaType(JsonNode value)
    {
      return value.asBoolean();
    }

    @Override
    protected JsonNode castJsonType(Object value)
    {
      return BooleanNode.valueOf((Boolean)value);
    }

    @Override
    protected Object parse(String s)
    {
      return Boolean.parseBoolean(s);
    }
  },

  INTEGER(Integer.class)
  {
    @Override
    public boolean isJsonType(JsonNode value)
    {
      return value != null && value.isInt();
    }

    @Override
    protected Object get(ResultSet resultSet, String columnName) throws SQLException
    {
      int value = resultSet.getInt(columnName);
      return resultSet.wasNull() ? null : value;
    }

    @Override
    protected Object castJavaType(JsonNode value)
    {
      return value.asInt();
    }

    @Override
    protected JsonNode castJsonType(Object value)
    {
      return IntNode.valueOf((Integer)value);
    }

    @Override
    protected Object parse(String s)
    {
      return Integer.parseInt(s);
    }
  },

  LONG(Long.class)
  {
    @Override
    public boolean isJsonType(JsonNode value)
    {
      return value != null && value.isLong();
    }

    @Override
    public JsonNode coerce(JsonNode value)
    {
      if (!value.canConvertToLong()) throw new IllegalArgumentException("Cannot coerce to long: " + value);
      return LongNode.valueOf(value.asLong());
    }

    @Override
    protected Object get(ResultSet resultSet, String columnName) throws SQLException
    {
      long value = resultSet.getLong(columnName);
      return resultSet.wasNull() ? null : value;
    }

    @Override
    protected Object castJavaType(JsonNode value)
    {
      return value.asLong();
    }

    @Override
    protected JsonNode castJsonType(Object value)
    {
      return LongNode.valueOf((Long)value);
    }

    @Override
    protected Object parse(String s)
    {
      return Long.parseLong(s);
    }
  },

  FLOAT(Float.class)
  {
    @Override
    public boolean isJsonType(JsonNode value)
    {
      return value != null && value.isFloat();
    }

    @Override
    public JsonNode coerce(JsonNode value)
    {
      if (!value.isDouble()) throw new IllegalArgumentException("Cannot coerce to float: " + value);

      double d = value.asDouble();
      if (d > Float.MAX_VALUE || d < Float.MIN_VALUE) throw new IllegalArgumentException("Cannot coerce to float: " + value);
      return FloatNode.valueOf((float)value.asDouble());
    }

    @Override
    protected Object get(ResultSet resultSet, String columnName) throws SQLException
    {
      float value = resultSet.getFloat(columnName);
      return resultSet.wasNull() ? null : value;
    }

    @Override
    protected Object castJavaType(JsonNode value)
    {
      return value.floatValue();
    }

    @Override
    protected JsonNode castJsonType(Object value)
    {
      return FloatNode.valueOf((Float)value);
    }

    @Override
    protected Object parse(String s)
    {
      return Float.parseFloat(s);
    }
  },

  DOUBLE(Double.class)
  {
    @Override
    public boolean isJsonType(JsonNode value)
    {
      return value != null && value.isDouble();
    }

    @Override
    protected Object get(ResultSet resultSet, String columnName) throws SQLException
    {
      double value = resultSet.getDouble(columnName);
      return resultSet.wasNull() ? null : value;
    }

    @Override
    protected Object castJavaType(JsonNode value)
    {
      return value.asDouble();
    }

    @Override
    protected JsonNode castJsonType(Object value)
    {
      return DoubleNode.valueOf((Double)value);
    }

    @Override
    protected Object parse(String s)
    {
      return Double.parseDouble(s);
    }
  },

  DECIMAL(BigDecimal.class)
  {
    @Override
    public boolean isJsonType(JsonNode value)
    {
      return value != null && value.isBigDecimal();
    }

    @Override
    public JsonNode coerce(JsonNode value)
    {
      if (!value.isDouble()) throw new IllegalArgumentException("Cannot coerce to float: " + value);
      return DecimalNode.valueOf(BigDecimal.valueOf(value.asDouble()));
    }

    @Override
    protected Object get(ResultSet resultSet, String columnName) throws SQLException
    {
      BigDecimal value = resultSet.getBigDecimal(columnName);
      return resultSet.wasNull() ? null : value;
    }

    @Override
    protected Object castJavaType(JsonNode value)
    {
      return value.decimalValue();
    }

    @Override
    protected JsonNode castJsonType(Object value)
    {
      return DecimalNode.valueOf((BigDecimal)value);
    }

    @Override
    protected Object parse(String s)
    {
      return new BigDecimal(s);
    }
  },

  TEXT(String.class)
  {
    @Override
    public boolean isJsonType(JsonNode value)
    {
      return value != null && value.isTextual();
    }

    @Override
    protected Object get(ResultSet resultSet, String columnName) throws SQLException
    {
      String value = resultSet.getString(columnName);
      return resultSet.wasNull() ? null : value;
    }

    @Override
    protected Object castJavaType(JsonNode value)
    {
      return value.asText();
    }

    @Override
    protected JsonNode castJsonType(Object value)
    {
      return TextNode.valueOf((String)value);
    }

    @Override
    protected Object parse(String s)
    {
      return s;
    }
  },

  DATE(Date.class)
  {
    @Override
    public boolean isJsonType(JsonNode value)
    {
      try
      {
        return value != null && value.isTextual() && Date.valueOf(value.asText()) != null;
      }
      catch (IllegalArgumentException e)
      {
        return false;
      }
    }

    @Override
    protected Object get(ResultSet resultSet, String columnName) throws SQLException
    {
      Date value = resultSet.getDate(columnName);
      return resultSet.wasNull() ? null : value;
    }

    @Override
    protected Object castJavaType(JsonNode value)
    {
      return Date.valueOf(value.asText());
    }

    @Override
    protected JsonNode castJsonType(Object value)
    {
      return TextNode.valueOf(value.toString());
    }

    @Override
    protected Object parse(String s)
    {
      return Date.valueOf(s);
    }
  },

  TIME(Time.class)
  {
    @Override
    public boolean isJsonType(JsonNode value)
    {
      try
      {
        //noinspection ConstantValue
        return value != null && value.isTextual() && Time.valueOf(LocalTime.parse(value.asText())) != null;
      }
      catch (IllegalArgumentException e)
      {
        return false;
      }
    }

    @Override
    protected Object get(ResultSet resultSet, String columnName) throws SQLException
    {
      Time value = resultSet.getTime(columnName);
      return resultSet.wasNull() ? null : value;
    }

    @Override
    protected Object castJavaType(JsonNode value)
    {
      return Time.valueOf(value.asText());
    }

    @Override
    protected JsonNode castJsonType(Object value)
    {
      return TextNode.valueOf(value.toString());
    }

    @Override
    protected Object parse(String s)
    {
      return Time.valueOf(LocalTime.parse(s));
    }
  },

  TIMESTAMP(Timestamp.class)
  {
    @Override
    public boolean isJsonType(JsonNode value)
    {
      try
      {
        //noinspection ConstantValue
        return value != null && value.isTextual() && Timestamp.valueOf(LocalDateTime.parse(value.asText())) != null;
      }
      catch (IllegalArgumentException e)
      {
        return false;
      }
    }

    @Override
    protected Object get(ResultSet resultSet, String columnName) throws SQLException
    {
      Timestamp value = resultSet.getTimestamp(columnName);
      return resultSet.wasNull() ? null : value;
    }

    @Override
    protected Object castJavaType(JsonNode value)
    {
      return Timestamp.valueOf(LocalDateTime.parse(value.asText()));
    }

    @Override
    protected JsonNode castJsonType(Object value)
    {
      return TextNode.valueOf(((Timestamp)value).toLocalDateTime().toString());
    }

    @Override
    protected Object parse(String s)
    {
      return Timestamp.valueOf(LocalDateTime.parse(s));
    }
  };

  private final Class<?> javaType;

  FieldType(Class<?> javaType)
  {
    this.javaType = javaType;
  }

  public Class<?> javaType()
  {
    return javaType;
  }

  public <T> T asJavaType(JsonNode value)
  {
    if (!isJsonType(value)) throw new IllegalArgumentException("Value " + value + " is not of type " + this);

    //noinspection unchecked
    return (T)castJavaType(value);
  }

  public <T extends JsonNode> T asJsonType(Object value)
  {
    if (value == null) return null;
    
    if (!isJavaType(value)) throw new IllegalArgumentException("Value " + value + " is not of type " + this);

    //noinspection unchecked
    return (T)castJsonType(value);
  }

  public boolean isJavaType(Object value)
  {
    return value != null && value.getClass() == javaType;
  }
  
  public abstract boolean isJsonType(JsonNode value);

  public Field field(String name, Column column)
  {
    return new Field(name, this, column);
  }

  public JsonNode coerce(JsonNode value)
  {
    return value;
  }

  protected abstract Object get(ResultSet resultSet, String columnName) throws SQLException;

  protected abstract Object castJavaType(JsonNode value);

  protected abstract JsonNode castJsonType(Object value);

  protected abstract Object parse(String s);
}
