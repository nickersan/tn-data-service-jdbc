package com.tn.service.data.jdbc.config;

import java.util.Collection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;

import com.tn.service.data.io.DefaultJsonCodec;
import com.tn.service.data.io.JsonCodec;
import com.tn.service.data.jdbc.domain.Field;
import com.tn.service.data.jdbc.io.Base64IdentityParser;
import com.tn.service.data.parameter.IdentityParser;
import com.tn.service.data.parameter.QueryBuilder;

@Configuration
@Profile("!api-integration-test")
class ParserConfiguration
{
  @Bean
  @Lazy
  IdentityParser<String, ?> identityParser(Collection<Field> fields, ObjectMapper objectMapper)
  {
    return new Base64IdentityParser(fields, objectMapper);
  }

  @Bean
  JsonCodec<ObjectNode> jsonCodec(ObjectMapper objectMapper)
  {
    return new DefaultJsonCodec<>(objectMapper, ObjectNode.class);
  }

  @Bean
  @Lazy
  QueryBuilder queryBuilder(Collection<Field> fields)
  {
    return new QueryBuilder(fields.stream().map(Field::name).toList());
  }
}
