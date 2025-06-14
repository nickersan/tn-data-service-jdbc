package com.tn.service.data.jdbc.repository;

import static java.lang.Math.ceilDiv;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;
import static java.util.stream.StreamSupport.stream;

import static com.google.common.collect.Lists.partition;

import static com.tn.lang.Iterables.asList;
import static com.tn.lang.Iterables.isEmpty;
import static com.tn.lang.Iterables.size;
import static com.tn.lang.Strings.repeat;
import static com.tn.lang.util.function.Lambdas.unwrapException;
import static com.tn.lang.util.function.Lambdas.wrapConsumer;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.transaction.annotation.Transactional;

import com.tn.lang.Iterables;
import com.tn.lang.sql.PreparedStatements;
import com.tn.lang.util.Page;
import com.tn.lang.util.function.ConsumerWithThrows;
import com.tn.lang.util.function.WrappedException;
import com.tn.query.QueryParser;
import com.tn.query.jdbc.JdbcPredicate;
import com.tn.service.data.domain.Direction;
import com.tn.service.data.jdbc.domain.Field;
import com.tn.service.data.repository.DataRepository;
import com.tn.service.data.repository.DeleteException;
import com.tn.service.data.repository.FindException;
import com.tn.service.data.repository.InsertException;
import com.tn.service.data.repository.UpdateException;

public class JdbcDataRepository implements DataRepository<ObjectNode, ObjectNode>
{
  private static final String COLUMN_PLACEHOLDER = "?";
  private static final String COLUMN_SEPARATOR = ", ";
  private static final int DEFAULT_BATCH_SIZE = 50;
  private static final String FIELD_PLACEHOLDER = "%s = ?";
  private static final String LOGICAL_AND = " AND ";
  private static final String LOGICAL_OR = " OR ";
  private static final String PARENTHESIS = "(%s)";
  private static final String SELECT = "SELECT %s FROM %s.%s";
  private static final String COUNT = "SELECT COUNT(*) FROM %s.%s";
  private static final String INSERT = "INSERT INTO %s.%s(%s) VALUES (%s)";
  private static final String UPDATE = "UPDATE %s.%s SET %s WHERE %s";
  private static final String DELETE = "DELETE FROM %s.%s";
  private static final String WHERE = "%s WHERE %s";
  private static final String ORDER_BY = "%s ORDER BY %s ASC";
  private static final String ORDER_BY_DESCENDING = "%s ORDER BY %s DESC";
  private static final String OFFSET = "%s OFFSET %d ROWS FETCH NEXT %d ROWS ONLY";

  private final ExecutorService queryExecutor;
  private final JdbcTemplate jdbcTemplate;
  private final Collection<Field> fields;
  private final Collection<Field> autoIncrementFields;
  private final Collection<Field> keyFields;
  private final Collection<Field> mutableFields;
  private final QueryParser<JdbcPredicate> queryParser;
  private final String selectSql;
  private final String countSql;
  private final String insertSql;
  private final String deleteSql;
  private final String schema;
  private final String table;
  private final String keyPredicate;

  private int batchSize = DEFAULT_BATCH_SIZE;

  public JdbcDataRepository(
    ExecutorService queryExecutor,
    JdbcTemplate jdbcTemplate,
    String schema,
    String table,
    Collection<Field> fields,
    QueryParser<JdbcPredicate> queryParser
  )
  {
    this.queryExecutor = queryExecutor;
    this.jdbcTemplate = jdbcTemplate;
    this.schema = schema;
    this.table = table;
    this.fields = fields;
    this.queryParser = queryParser;

    this.autoIncrementFields = fields.stream().filter(field -> field.column().autoIncrement()).toList();
    this.keyFields = fields.stream().filter(field -> field.column().key()).toList();
    this.mutableFields = fields.stream().filter(field -> !field.column().key()).toList();

    this.keyPredicate = keyFields.stream().map(field -> format(FIELD_PLACEHOLDER, field.column().name())).collect(joining(LOGICAL_AND));

    this.selectSql = selectSql(schema, table, fields);
    this.countSql = countSql(schema, table);
    this.insertSql = insertSql(schema, table, fields);
    this.deleteSql = deleteSql(schema, table);
  }

  public JdbcDataRepository withBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
    return this;
  }

  @Override
  public Optional<ObjectNode> find(ObjectNode key) throws FindException
  {
    try
    {
      return jdbcTemplate.query(
        where(selectSql, keyPredicate),
        preparedStatement -> setValues(preparedStatement, parameterIndex(), key, keyFields),
        this::object
      ).stream().findFirst();
    }
    catch (DataAccessException e)
    {
      throw new FindException(e.getCause());
    }
  }

  @Override
  public Collection<ObjectNode> findAll(Iterable<String> sort, Direction direction) throws FindException
  {
    try
    {
      return jdbcTemplate.query(orderBy(selectSql, sort, direction), this::object);
    }
    catch (DataAccessException e)
    {
      throw new FindException(e.getCause());
    }
  }

  @Override
  public Collection<ObjectNode> findAll(Iterable<ObjectNode> keys) throws FindException
  {
    try
    {
      AtomicInteger parameterIndex = parameterIndex();
      return jdbcTemplate.query(
        where(selectSql, repeat(format(PARENTHESIS, keyPredicate), LOGICAL_OR, size(keys))),
        preparedStatement -> keys.forEach(wrapConsumer(key -> setValues(preparedStatement, parameterIndex, key, keyFields))),
        this::object
      );
    }
    catch (DataAccessException e)
    {
      throw new DeleteException(e.getCause());
    }
    catch (WrappedException e)
    {
      Throwable unwrapped = unwrapException(e);
      if (unwrapped instanceof RuntimeException) throw (RuntimeException)unwrapped;
      else throw new FindException(unwrapped);
    }
  }

  @Override
  public Page<ObjectNode> findAll(int pageNumber, int pageSize, Iterable<String> sort, Direction direction) throws FindException
  {
    try
    {
      Future<Collection<ObjectNode>> objectsFuture = queryExecutor.submit(
        () -> jdbcTemplate.query(
          paginated(orderBy(selectSql, sort, direction), pageNumber, pageSize),
          this::object
        )
      );
      @SuppressWarnings("DataFlowIssue")
      int count = jdbcTemplate.query(countSql, this::count);

      return new Page<>(
        objectsFuture.get(),
        pageNumber,
        pageSize,
        count,
        ceilDiv(count, pageSize)
      );
    }
    catch (ExecutionException e)
    {
      throw new FindException(e.getCause().getCause());
    }
    catch (InterruptedException e)
    {
      throw new FindException(e);
    }
  }

  @Override
  public Collection<ObjectNode> findWhere(String query, Iterable<String> sort, Direction direction) throws FindException
  {
    try
    {
      JdbcPredicate predicate = queryParser.parse(query);

      //noinspection SqlSourceToSinkFlow
      return jdbcTemplate.query(
        orderBy(where(selectSql, predicate.toSql()), sort, direction),
        predicate::setValues,
        this::object
      );
    }
    catch (DataAccessException e)
    {
      throw new FindException(e.getCause());
    }
  }

  @Override
  public Page<ObjectNode> findWhere(String query, int pageNumber, int pageSize, Iterable<String> sort, Direction direction) throws FindException
  {
    try
    {
      JdbcPredicate predicate = queryParser.parse(query);

      Future<Collection<ObjectNode>> objectsFuture = queryExecutor.submit(
        () -> jdbcTemplate.query(
          paginated(orderBy(where(selectSql, predicate.toSql()), sort, direction), pageNumber, pageSize),
          predicate::setValues,
          this::object
        )
      );
      @SuppressWarnings({"DataFlowIssue", "SqlSourceToSinkFlow"})
      int count = jdbcTemplate.query(where(countSql, predicate.toSql()), predicate::setValues, this::count);

      return new Page<>(
        objectsFuture.get(),
        pageNumber,
        pageSize,
        count,
        ceilDiv(count, pageSize)
      );
    }
    catch (ExecutionException e)
    {
      throw new FindException(e.getCause().getCause());
    }
    catch (InterruptedException e)
    {
      throw new FindException(e);
    }
  }

  @Override
  @Transactional
  public ObjectNode insert(ObjectNode object) throws InsertException
  {
    try
    {
      KeyHolder keyHolder = new GeneratedKeyHolder();

      jdbcTemplate.update(
        connection ->
        {
          PreparedStatement preparedStatement = connection.prepareStatement(
            insertSql,
            autoIncrementFields.isEmpty() ? PreparedStatement.NO_GENERATED_KEYS : PreparedStatement.RETURN_GENERATED_KEYS
          );
          AtomicInteger parameterIndex = parameterIndex();
          setValues(preparedStatement, parameterIndex, object, keyFields.stream().filter(keyField -> !keyField.column().autoIncrement()).toList());
          setValues(preparedStatement, parameterIndex, object, mutableFields);
          return preparedStatement;
        },
        keyHolder
      );

      if (autoIncrementFields.isEmpty()) return object;

      return withIdentifiers(object, keyHolder.getKeyList().getFirst());
    }
    catch (DataAccessException e)
    {
      throw new InsertException(e.getCause());
    }
  }

  @Override
  @Transactional
  public Collection<ObjectNode> insertAll(Iterable<ObjectNode> objects) throws InsertException
  {
    if (isEmpty(objects)) return emptyList();
    if (!(objects instanceof List)) return insertAll(asList(objects));

    try
    {
      List<Field> insertableKeyFields = keyFields.stream().filter(keyField -> !keyField.column().autoIncrement()).toList();
      Collection<ObjectNode> persistedObjects = new ArrayList<>();

      partition((List<ObjectNode>)objects, batchSize).forEach(
        batch ->
        {
          KeyHolder keyHolder = new GeneratedKeyHolder();

          jdbcTemplate.batchUpdate(
            connection -> connection.prepareStatement(
              insertSql,
              autoIncrementFields.isEmpty() ? PreparedStatement.NO_GENERATED_KEYS : PreparedStatement.RETURN_GENERATED_KEYS
            ),
            batchPreparedStatementSetter(batch, insertableKeyFields, mutableFields),
            keyHolder
          );

          persistedObjects.addAll(autoIncrementFields.isEmpty() ? batch : withIdentifiers(batch, keyHolder.getKeyList()));
        }
      );

      return persistedObjects;
    }
    catch (DataAccessException e)
    {
      throw new InsertException(e.getCause());
    }
  }

  @Override
  @Transactional
  public ObjectNode update(ObjectNode object) throws UpdateException
  {
    try
    {
      Collection<Field> mutableFields = mutableFields(object);
      if (mutableFields.isEmpty()) throw new UpdateException("Unrecognized object: " + object);

      jdbcTemplate.update(
        updateSql(mutableFields),
        preparedStatement ->
        {
          AtomicInteger parameterIndex = parameterIndex();
          setValues(preparedStatement, parameterIndex, object, mutableFields);
          setValues(preparedStatement, parameterIndex, object, keyFields);
        }
      );

      return find(object).orElseThrow(() -> new UpdateException("Failed to find object after update: " + object));
    }
    catch (DataAccessException e)
    {
      throw new UpdateException(e.getCause());
    }
  }

  @Override
  @Transactional
  public Collection<ObjectNode> updateAll(Iterable<ObjectNode> objects) throws UpdateException
  {
    if (isEmpty(objects)) return emptyList();

    stream(objects.spliterator(), false).collect(groupingBy(this::mutableFields)).forEach(
      (mutableFields, objectsForMutableFields) ->
      {
        if (mutableFields.isEmpty()) throw new UpdateException("Unrecognized objects: " + objectsForMutableFields);
        String updateSql = updateSql(mutableFields);

        partition(objectsForMutableFields, batchSize).forEach(
          batch -> jdbcTemplate.batchUpdate(updateSql, batchPreparedStatementSetter(batch, mutableFields, keyFields))
        );
      }
    );

    return findAll(objects);
  }

  @Override
  @Transactional
  public Optional<ObjectNode> delete(ObjectNode key) throws DeleteException
  {
    try
    {
      ObjectNode object = find(key).orElse(null);
      if (object == null) return Optional.empty();

      jdbcTemplate.update(
        where(deleteSql, keyPredicate),
        preparedStatement -> setValues(preparedStatement, parameterIndex(), key, keyFields)
      );

      return Optional.of(object);
    }
    catch (DataAccessException e)
    {
      throw new DeleteException(e.getCause());
    }
  }

  @Override
  @Transactional
  public Collection<ObjectNode> deleteAll(Iterable<ObjectNode> keys) throws DeleteException
  {
    if (isEmpty(keys)) return emptyList();

    Collection<ObjectNode> objects = findAll(keys);
    if (objects.size() != Iterables.size(keys)) return emptyList();

    try
    {
      AtomicInteger parameterIndex = parameterIndex();
      jdbcTemplate.update(
        where(deleteSql, repeat(format(PARENTHESIS, keyPredicate), LOGICAL_OR, size(keys))),
        preparedStatement -> keys.forEach(wrapConsumer(key -> setValues(preparedStatement, parameterIndex, key, keyFields)))
      );

      return objects;
    }
    catch (DataAccessException e)
    {
      throw new DeleteException(e.getCause());
    }
    catch (WrappedException e)
    {
      Throwable unwrapped = unwrapException(e);
      if (unwrapped instanceof RuntimeException) throw (RuntimeException)unwrapped;
      else throw new DeleteException(unwrapped);
    }
  }

  @SafeVarargs
  private BatchPreparedStatementSetter batchPreparedStatementSetter(List<ObjectNode> objects, Collection<Field>... fieldGroups)
  {
    return new BatchPreparedStatementSetter()
    {
      @Override
      public void setValues(@Nonnull PreparedStatement preparedStatement, int index) throws SQLException
      {
        AtomicInteger parameterIndex = parameterIndex();
        for (Collection<Field> fieldGroup : fieldGroups) JdbcDataRepository.this.setValues(preparedStatement, parameterIndex, objects.get(index), fieldGroup);
      }

      @Override
      public int getBatchSize()
      {
        return objects.size();
      }
    };
  }

  private List<ObjectNode> withIdentifiers(List<ObjectNode> objects, List<Map<String, Object>> identifiers)
  {
    if (identifiers.size() != objects.size()) throw new InsertException("Identifier mismatch after insert");

    return IntStream.range(0, objects.size())
      .mapToObj(i -> withIdentifiers(objects.get(i), identifiers.get(i)))
      .toList();
  }

  private ObjectNode withIdentifiers(ObjectNode object, Map<String, Object> identifiers)
  {
    ObjectNode objectWithIdentifier = new ObjectNode(null);

    for (Field field : fields)
    {
      if (field.column().autoIncrement()) field.set(objectWithIdentifier, identifiers.get(field.column().name()));
      else if (object.has(field.name())) objectWithIdentifier.set(field.name(), object.get(field.name()));
    }

    return objectWithIdentifier;
  }

  private ObjectNode object(ResultSet resultSet, int i)
  {
    ObjectNode object = new ObjectNode(null);
    keyFields.forEach(field -> setField(object, field.name(), get(field, resultSet)));
    mutableFields.forEach(field -> setField(object, field.name(), get(field, resultSet)));

    return object;
  }

  private int count(ResultSet resultSet) throws SQLException
  {
    return resultSet.next() ? resultSet.getInt(1) : 0;
  }

  private Collection<Field> mutableFields(ObjectNode object)
  {
    return mutableFields.stream().filter(field -> object.has(field.name())).toList();
  }

  private void setField(ObjectNode object, String name, JsonNode value)
  {
    if (value != null) object.set(name, value);
  }

  private JsonNode get(Field field, ResultSet resultSet)
  {
    try
    {
      return field.type().asJsonType(field.getAsJavaType(resultSet));
    }
    catch (SQLException e)
    {
      throw new FindException(e);
    }
  }

  private void setValues(PreparedStatement preparedStatement, AtomicInteger index, ObjectNode object, Collection<Field> fields) throws SQLException
  {
    try
    {
      fields.forEach(wrapConsumer(setValue(preparedStatement, index, object)));
    }
    catch (WrappedException e)
    {
      throw (SQLException)e.getCause();
    }
  }

  private ConsumerWithThrows<Field, SQLException> setValue(PreparedStatement preparedStatement, AtomicInteger index, ObjectNode object)
  {
    return field -> setValue(preparedStatement, index, field, field.getAsJavaType(object));
  }

  private void setValue(PreparedStatement preparedStatement, AtomicInteger index, Field field, Object value) throws SQLException
  {
    if (value != null) PreparedStatements.setValue(preparedStatement, index::getAndIncrement, value);
    else preparedStatement.setNull(index.getAndIncrement(), field.column().type());
  }

  private String selectSql(String schema, String table, Collection<Field> fields)
  {
    return format(
      SELECT,
      fields.stream().map(field -> field.column().name()).collect(joining(COLUMN_SEPARATOR)),
      schema,
      table
    );
  }

  private String countSql(String schema, String table)
  {
    return format(
      COUNT,
      schema,
      table
    );
  }

  private String insertSql(String schema, String table, Collection<Field> fields)
  {
    Collection<Field> insertableFields = fields.stream().filter(field -> !field.column().autoIncrement()).toList();

    return format(
      INSERT,
      schema,
      table,
      insertableFields.stream().map(insertableField -> insertableField.column().name()).collect(joining(COLUMN_SEPARATOR)),
      (COLUMN_PLACEHOLDER + COLUMN_SEPARATOR).repeat(insertableFields.size() - 1) + COLUMN_PLACEHOLDER
    );
  }

  private String updateSql(Collection<Field> updatableFields)
  {
    return format(
      UPDATE,
      schema,
      table,
      updatableFields.stream().map(field -> format(FIELD_PLACEHOLDER, field.column().name())).collect(joining(COLUMN_SEPARATOR)),
      keyPredicate
    );
  }

  private String deleteSql(String schema, String table)
  {
    return format(
      DELETE,
      schema,
      table
    );
  }

  private String where(String sql, String predicate)
  {
    return WHERE.formatted(sql, predicate);
  }

  private String orderBy(String sql, Iterable<String> sort, Direction direction)
  {
    return format(
      direction.isDescending() ? ORDER_BY_DESCENDING : ORDER_BY,
      sql,
      sortFields(Iterables.asSet(sort)).stream().map(field -> field.column().name()).collect(joining(COLUMN_SEPARATOR))
    );
  }

  private Collection<Field> sortFields(Collection<String> fieldNames)
  {
    return fieldNames.isEmpty()
      ? keyFields
      : fields.stream().filter(field -> fieldNames.contains(field.name())).collect(toSet());
  }

  private String paginated(String sql, int pageNumber, int pageSize)
  {
    return format(OFFSET, sql, pageNumber * pageSize, pageSize);
  }

  private AtomicInteger parameterIndex()
  {
    return new AtomicInteger(1);
  }
}
