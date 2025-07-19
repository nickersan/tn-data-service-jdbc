package com.tn.service.data.jdbc.config;

import static java.util.stream.Collectors.toMap;

import java.util.Collection;
import java.util.concurrent.Executors;
import javax.sql.DataSource;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;

import com.tn.query.DefaultQueryParser;
import com.tn.query.ValueMappers;
import com.tn.query.jdbc.JdbcPredicateFactory;
import com.tn.service.data.jdbc.domain.Field;
import com.tn.service.data.jdbc.repository.FieldRepository;
import com.tn.service.data.jdbc.repository.JdbcDataRepository;
import com.tn.service.data.jdbc.repository.JdbcFieldRepository;
import com.tn.service.data.repository.DataRepository;

@Configuration
@Profile("!api-integration-test")
class RepositoryConfiguration
{
  @Bean
  @Lazy
  @ConditionalOnProperty({"tn.data.schema", "tn.data.table"})
  Collection<Field> fields(
    FieldRepository fieldRepository,
    @Value("${tn.data.schema}")
    String schema,
    @Value("${tn.data.table}")
    String table
  )
  {
    Collection<Field> fields = fieldRepository.findForTable(schema, table);
    if (fields.isEmpty()) throw new IllegalStateException("No such table: " + schema + "." + table);

    return fields;
  }

  @Bean
  @Lazy
  @ConditionalOnProperty({"tn.data.schema", "tn.data.table"})
  DataRepository<ObjectNode, ObjectNode> dataRepository(
    JdbcTemplate jdbcTemplate,
    Collection<Field> fields,
    @Value("${tn.data.schema}")
    String schema,
    @Value("${tn.data.table}")
    String table,
    @Value("${tn.data.parallelism:10}")
    int parallelism
  )
  {
    return new JdbcDataRepository(
      Executors.newWorkStealingPool(parallelism),
      jdbcTemplate,
      schema,
      table,
      fields,
      new DefaultQueryParser<>(
        new JdbcPredicateFactory(fields.stream().collect(toMap(Field::name, field -> field.column().name()))),
        ValueMappers.forFields(fields.stream().map(field -> new ValueMappers.Field(field.name(), field.type().javaType())).toList())
      )
    );
  }

  @Bean
  FieldRepository fieldRepository(DataSource dataSource)
  {
    return new JdbcFieldRepository(dataSource);
  }
}
