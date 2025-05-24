package com.tn.service.data.jdbc.config;

import java.util.Collection;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Profile;

import com.tn.service.data.io.KeyParser;
import com.tn.service.data.jdbc.domain.Field;
import com.tn.service.data.jdbc.io.Base64KeyParser;

@Configuration
@Profile("!api-integration-test")
class ParserConfiguration
{
  @Bean
  @Lazy
  KeyParser<?> keyParser(Collection<Field> fields, ObjectMapper objectMapper)
  {
    return new Base64KeyParser(fields, objectMapper);
  }
}
