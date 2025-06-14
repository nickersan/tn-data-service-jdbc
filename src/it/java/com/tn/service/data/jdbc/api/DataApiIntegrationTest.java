package com.tn.service.data.jdbc.api;

import static java.lang.String.format;
import static java.util.Collections.emptySet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyIterable;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static com.tn.service.data.controller.DataController.DEFAULT_PAGE_NUMBER;
import static com.tn.service.data.controller.DataController.DEFAULT_PAGE_SIZE;
import static com.tn.service.data.controller.DataController.FIELD_MESSAGE;
import static com.tn.service.data.domain.Direction.ASCENDING;
import static com.tn.service.data.domain.Direction.DESCENDING;

import java.io.IOException;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.StreamSupport;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentMatcher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.util.UriComponentsBuilder;

import com.tn.lang.util.Page;
import com.tn.service.data.io.DefaultJsonCodec;
import com.tn.service.data.io.JsonCodec;
import com.tn.service.data.parameter.IdentityParser;
import com.tn.service.data.parameter.QueryBuilder;
import com.tn.service.data.repository.DataRepository;
import com.tn.service.data.repository.DeleteException;
import com.tn.service.data.repository.InsertException;
import com.tn.service.data.repository.UpdateException;

@SpringBootTest(
  webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
  classes = DataApiIntegrationTest.TestConfiguration.class
)
@SuppressWarnings("SpringBootApplicationProperties")
@EnableAutoConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class DataApiIntegrationTest
{
  private static final String FIELD_ID = "id";
  private static final String FIELD_ID_2 = "id2";
  private static final String FIELD_NAME = "name";
  private static final ParameterizedTypeReference<List<ObjectNode>> TYPE_REFERENCE_LIST = new ParameterizedTypeReference<>() {};
  private static final ParameterizedTypeReference<Page<ObjectNode>> TYPE_REFERENCE_PAGE = new ParameterizedTypeReference<>() {};

  @Autowired
  TestRestTemplate testRestTemplate;

  @Autowired
  DataRepository<ObjectNode, ObjectNode> dataRepository;

  @Autowired
  IdentityParser<String, ObjectNode> identityParser;

  @Autowired
  ObjectMapper objectMapper;

  @Test
  void shouldGet()
  {
    ObjectNode data1 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_NAME, TextNode.valueOf("Data 1")));
    ObjectNode data2 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(2), FIELD_NAME, TextNode.valueOf("Data 2")));

    when(dataRepository.findAll(emptySet(), ASCENDING)).thenReturn(List.of(data1, data2));

    ResponseEntity<ArrayNode> response = testRestTemplate.getForEntity("/", ArrayNode.class);

    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertEquals(new ArrayNode(null, List.of(data1, data2)), response.getBody());
  }

  @Test
  void shouldGetWithSort()
  {
    ObjectNode data1 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_NAME, TextNode.valueOf("Data 1")));
    ObjectNode data2 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(2), FIELD_NAME, TextNode.valueOf("Data 2")));

    when(dataRepository.findAll(Set.of(FIELD_NAME), DESCENDING)).thenReturn(List.of(data1, data2));

    ResponseEntity<ArrayNode> response = testRestTemplate.getForEntity(
      format("/?$sort=%s&$direction=%s", FIELD_NAME, DESCENDING),
      ArrayNode.class
    );

    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertEquals(new ArrayNode(null, List.of(data1, data2)), response.getBody());
  }

  @Test
  void shouldGetWithSimpleId()
  {
    ObjectNode id = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1)));
    ObjectNode data = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_NAME, TextNode.valueOf("Data 1")));

    when(identityParser.parse(id.get(FIELD_ID).asText())).thenReturn(id);
    when(dataRepository.find(id)).thenReturn(Optional.of(data));

    ResponseEntity<ObjectNode> response = testRestTemplate.getForEntity(
      format("/%s", id.get(FIELD_ID).asText()),
      ObjectNode.class
    );

    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertEquals(data, response.getBody());
  }

  @Test
  void shouldGetWithSimpleIds()
  {
    ObjectNode id1 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1)));
    ObjectNode id2 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(2)));
    ObjectNode data1 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_NAME, TextNode.valueOf("Data 1")));
    ObjectNode data2 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(2), FIELD_NAME, TextNode.valueOf("Data 2")));

    when(identityParser.parse(id1.get(FIELD_ID).asText())).thenReturn(id1);
    when(identityParser.parse(id2.get(FIELD_ID).asText())).thenReturn(id2);
    when(dataRepository.findAll(Set.of(id1, id2), emptySet(), ASCENDING)).thenReturn(List.of(data1, data2));

    String url = UriComponentsBuilder.fromPath("/")
      .queryParam(FIELD_ID, List.of(id1.get(FIELD_ID).asText(), id2.get(FIELD_ID).asText()))
      .encode()
      .toUriString();

    ResponseEntity<ArrayNode> response = testRestTemplate.getForEntity(url, ArrayNode.class);

    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertEquals(new ArrayNode(null, List.of(data1, data2)), response.getBody());
  }

  @Test
  void shouldGetWithComplexId() throws Exception
  {
    ObjectNode id = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_ID_2, TextNode.valueOf("A")));
    ObjectNode data = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_ID_2, TextNode.valueOf("A"), FIELD_NAME, TextNode.valueOf("Data 1")));

    String encodedId = encode(id);

    when(identityParser.parse(encodedId)).thenReturn(id);
    when(dataRepository.find(id)).thenReturn(Optional.of(data));

    ResponseEntity<ObjectNode> response = testRestTemplate.getForEntity("/" + encodedId, ObjectNode.class);

    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertEquals(data, response.getBody());
  }

  @Test
  void shouldGetWithComplexIds() throws Exception
  {
    ObjectNode id1 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_ID_2, TextNode.valueOf("A")));
    ObjectNode id2 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(2), FIELD_ID_2, TextNode.valueOf("B")));
    ObjectNode data1 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_ID_2, TextNode.valueOf("A"), FIELD_NAME, TextNode.valueOf("Data 1")));
    ObjectNode data2 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(2), FIELD_ID_2, TextNode.valueOf("B"), FIELD_NAME, TextNode.valueOf("Data 2")));

    String encodedId1 = encode(id1);
    String encodedId2 = encode(id2);

    when(identityParser.parse(encodedId1)).thenReturn(id1);
    when(identityParser.parse(encodedId2)).thenReturn(id2);
    when(dataRepository.findAll(Set.of(id1, id2), emptySet(), ASCENDING)).thenReturn(List.of(data1, data2));

    String url = UriComponentsBuilder.fromPath("/")
      .queryParam("id", List.of(encodedId1, encodedId2))
      .encode()
      .toUriString();

    ResponseEntity<ArrayNode> response = testRestTemplate.getForEntity(url, ArrayNode.class);

    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertEquals(new ArrayNode(null, List.of(data1, data2)), response.getBody());
  }

  @Test
  void shouldGetWithQuery()
  {
    ObjectNode data = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_NAME, TextNode.valueOf("Data 1")));
    String query = "name=Data 1";

    when(dataRepository.findWhere(query, emptySet(), ASCENDING)).thenReturn(List.of(data));

    ResponseEntity<ArrayNode> response = testRestTemplate.getForEntity("/?q=" + query, ArrayNode.class);

    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertEquals(new ArrayNode(null, List.of(data)), response.getBody());
  }

  @Test
  void shouldGetWithQueryAndPageNumber()
  {
    ObjectNode data = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_NAME, TextNode.valueOf("Data 1")));
    String query = "name=Data 1";
    int pageNumber = 1;

    Page<ObjectNode> page = new Page<>(List.of(data), pageNumber, DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE + 1, 2);

    when(dataRepository.findWhere(query, pageNumber, DEFAULT_PAGE_SIZE, emptySet(), ASCENDING)).thenReturn(page);

    ResponseEntity<Page<ObjectNode>> response = testRestTemplate.exchange(
      "/?q=" + query + "&$pageNumber=" + pageNumber,
      HttpMethod.GET,
      null,
      TYPE_REFERENCE_PAGE
    );

    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertEquals(page, response.getBody());
  }

  @Test
  void shouldGetWithQueryAndPageSize()
  {
    ObjectNode data = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_NAME, TextNode.valueOf("Data 1")));
    String query = "name=Data 1";
    int pageSize = 10;

    Page<ObjectNode> page = new Page<>(List.of(data), DEFAULT_PAGE_NUMBER, pageSize, pageSize + 1, 2);

    when(dataRepository.findWhere(query, DEFAULT_PAGE_NUMBER, pageSize, emptySet(), ASCENDING)).thenReturn(page);

    ResponseEntity<Page<ObjectNode>> response = testRestTemplate.exchange(
      "/?q=" + query + "&$pageSize=" + pageSize,
      HttpMethod.GET,
      null,
      TYPE_REFERENCE_PAGE
    );

    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertEquals(page, response.getBody());
  }

  @Test
  void shouldGetWithQueryAndPageNumberAndPageSize()
  {
    ObjectNode data = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_NAME, TextNode.valueOf("Data 1")));
    String query = "name=Data 1";
    int pageNumber = 1;
    int pageSize = 10;

    Page<ObjectNode> page = new Page<>(List.of(data), pageNumber, pageSize, pageSize + 1, 2);

    when(dataRepository.findWhere(query, pageNumber, pageSize, emptySet(), ASCENDING)).thenReturn(page);

    ResponseEntity<Page<ObjectNode>> response = testRestTemplate.exchange(
      "/?q=" + query + "&$pageNumber=" + pageNumber + "&$pageSize=" + pageSize,
      HttpMethod.GET,
      null,
      TYPE_REFERENCE_PAGE
    );

    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertEquals(page, response.getBody());
  }

  @Test
  void shouldNotGetForUnknownId()
  {
    ObjectNode id = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1)));

    when(identityParser.parse(id.get(FIELD_ID).asText())).thenReturn(id);
    when(dataRepository.find(id)).thenReturn(Optional.empty());

    ResponseEntity<ObjectNode> response = testRestTemplate.getForEntity("/" + id.get(FIELD_ID).asText(), ObjectNode.class);

    assertTrue(response.getStatusCode().isSameCodeAs(HttpStatus.NOT_FOUND));
  }

  @Test
  void shouldNotGetWithIdAndQuery()
  {
    ResponseEntity<ObjectNode> response = testRestTemplate.getForEntity("/?id=1&q=x", ObjectNode.class);

    assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    assertNotNull(response.getBody());
    assertEquals("Identity parameters can only be used in isolation from other non-sort parameters", response.getBody().get(FIELD_MESSAGE).asText());
  }

  @ParameterizedTest
  @ValueSource(strings = {"$pageNumber=0", "$pageSize=10", "$pageNumber=0&$pageSize=10"})
  void shouldNotGetWithIdAndPagination(String paginationParameter)
  {
    ResponseEntity<ObjectNode> response = testRestTemplate.getForEntity("/?id=1&" + paginationParameter, ObjectNode.class);

    assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    assertNotNull(response.getBody());
    assertEquals("Identity parameters can only be used in isolation from other non-sort parameters", response.getBody().get(FIELD_MESSAGE).asText());
  }

  @Test
  void shouldPostWithObject()
  {
    ObjectNode data = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_NAME, TextNode.valueOf("Data 1")));

    when(dataRepository.insert(data)).thenReturn(data);

    ResponseEntity<ObjectNode> response = testRestTemplate.postForEntity("/", data, ObjectNode.class);

    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertEquals(data, response.getBody());

    verify(dataRepository).insert(data);
  }

  @Test
  void shouldPostWithArray()
  {
    List<ObjectNode> data = List.of(
      objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_NAME, TextNode.valueOf("Data 1"))),
      objectNode(Map.of(FIELD_ID, IntNode.valueOf(2), FIELD_NAME, TextNode.valueOf("Data 2")))
    );

    when(dataRepository.insertAll(anyIterable())).thenReturn(data);

    ResponseEntity<List<ObjectNode>> response = testRestTemplate.exchange("/", HttpMethod.POST, body(data), TYPE_REFERENCE_LIST);

    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertEquals(data, response.getBody());

    verify(dataRepository).insertAll(argThat(containsAll(data)));
  }

  @Test
  void shouldNotPostWithInvalidBody()
  {
    ResponseEntity<ObjectNode> response = testRestTemplate.postForEntity("/", List.of("INVALID"), ObjectNode.class);

    assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    assertNotNull(response.getBody());
    assertEquals("Invalid body", response.getBody().get(FIELD_MESSAGE).asText());
  }

  @Test
  void shouldNotPostWithRepositoryError()
  {
    ObjectNode data = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_NAME, TextNode.valueOf("Data 1")));

    when(dataRepository.insert(data)).thenThrow(new InsertException("TESTING"));

    ResponseEntity<ObjectNode> response = testRestTemplate.postForEntity("/", data, ObjectNode.class);

    assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
    assertNotNull(response.getBody());
    assertEquals("TESTING", response.getBody().get(FIELD_MESSAGE).asText());
  }

  @Test
  void shouldPutWithObject()
  {
    ObjectNode data = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_NAME, TextNode.valueOf("Data 1")));

    when(dataRepository.update(data)).thenReturn(data);

    ResponseEntity<ObjectNode> response = testRestTemplate.exchange("/", HttpMethod.PUT, body(data), ObjectNode.class);

    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertEquals(data, response.getBody());

    verify(dataRepository).update(data);
  }

  @Test
  void shouldPutWithArray()
  {
    List<ObjectNode> data = List.of(
      objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_NAME, TextNode.valueOf("Data 1"))),
      objectNode(Map.of(FIELD_ID, IntNode.valueOf(2), FIELD_NAME, TextNode.valueOf("Data 2")))
    );

    when(dataRepository.updateAll(anyIterable())).thenReturn(data);

    ResponseEntity<List<ObjectNode>> response = testRestTemplate.exchange("/", HttpMethod.PUT, body(data), TYPE_REFERENCE_LIST);

    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertEquals(data, response.getBody());

    verify(dataRepository).updateAll(argThat(containsAll(data)));
  }

  @Test
  void shouldNotPutWithInvalidBody()
  {
    ResponseEntity<ObjectNode> response = testRestTemplate.exchange("/", HttpMethod.PUT, body(List.of("INVALID")), ObjectNode.class);

    assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    assertNotNull(response.getBody());
    assertEquals("Invalid body", response.getBody().get(FIELD_MESSAGE).asText());
  }

  @Test
  void shouldNotPutWithRepositoryError()
  {
    ObjectNode data = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_NAME, TextNode.valueOf("Data 1")));

    when(dataRepository.update(data)).thenThrow(new UpdateException("TESTING"));

    ResponseEntity<ObjectNode> response = testRestTemplate.exchange("/", HttpMethod.PUT, body(data), ObjectNode.class);

    assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
    assertNotNull(response.getBody());
    assertEquals("TESTING", response.getBody().get(FIELD_MESSAGE).asText());
  }

  @Test
  void shouldDeleteWithSimpleId()
  {
    ObjectNode id = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1)));
    ObjectNode data = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_NAME, TextNode.valueOf("Data 1")));

    when(identityParser.parse(id.get(FIELD_ID).asText())).thenReturn(id);
    when(dataRepository.delete(id)).thenReturn(Optional.of(data));

    ResponseEntity<ObjectNode> response = testRestTemplate.exchange("/" + id.get(FIELD_ID).asText(), HttpMethod.DELETE, null, ObjectNode.class);

    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertNotNull(response.getBody());
    assertEquals(data, response.getBody());
  }

  @Test
  void shouldDeleteWithSimpleIds()
  {
    ObjectNode id1 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1)));
    ObjectNode id2 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(2)));
    ObjectNode data1 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_NAME, TextNode.valueOf("Data 1")));
    ObjectNode data2 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(2), FIELD_NAME, TextNode.valueOf("Data 2")));

    when(identityParser.parse(id1.get(FIELD_ID).asText())).thenReturn(id1);
    when(identityParser.parse(id2.get(FIELD_ID).asText())).thenReturn(id2);
    when(dataRepository.deleteAll(Set.of(id1, id2))).thenReturn(List.of(data1, data2));

    String url = UriComponentsBuilder.fromPath("/")
      .queryParam("id", List.of(id1.get(FIELD_ID).asText(), id2.get(FIELD_ID).asText()))
      .encode()
      .toUriString();

    ResponseEntity<List<ObjectNode>> response = testRestTemplate.exchange(url, HttpMethod.DELETE, null, TYPE_REFERENCE_LIST);

    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertNotNull(response.getBody());
    assertEquals(List.of(data1, data2), response.getBody());
  }

  @Test
  void shouldDeleteWithComplexId() throws Exception
  {
    ObjectNode id = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_ID_2, TextNode.valueOf("A")));
    ObjectNode data = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_ID_2, TextNode.valueOf("A"), FIELD_NAME, TextNode.valueOf("Data 1")));

    String encodedId = encode(id);

    when(identityParser.parse(encodedId)).thenReturn(id);
    when(dataRepository.delete(id)).thenReturn(Optional.of(data));

    ResponseEntity<ObjectNode> response = testRestTemplate.exchange("/" + encodedId, HttpMethod.DELETE, null, ObjectNode.class);

    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertNotNull(response.getBody());
    assertEquals(data, response.getBody());
  }

  @Test
  void shouldDeleteWithComplexIds() throws Exception
  {
    ObjectNode id1 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_ID_2, TextNode.valueOf("A")));
    ObjectNode id2 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(2), FIELD_ID_2, TextNode.valueOf("B")));
    ObjectNode data1 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_ID_2, TextNode.valueOf("A"), FIELD_NAME, TextNode.valueOf("Data 1")));
    ObjectNode data2 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(2), FIELD_ID_2, TextNode.valueOf("B"), FIELD_NAME, TextNode.valueOf("Data 2")));

    String encodedId1 = encode(id1);
    String encodedId2 = encode(id2);

    when(identityParser.parse(encodedId1)).thenReturn(id1);
    when(identityParser.parse(encodedId2)).thenReturn(id2);
    when(dataRepository.deleteAll(Set.of(id1, id2))).thenReturn(List.of(data1, data2));

    String url = UriComponentsBuilder.fromPath("/")
      .queryParam("id", List.of(encodedId1, encodedId2))
      .encode()
      .toUriString();

    ResponseEntity<List<ObjectNode>> response = testRestTemplate.exchange(url, HttpMethod.DELETE, null, TYPE_REFERENCE_LIST);

    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertNotNull(response.getBody());
    assertEquals(List.of(data1, data2), response.getBody());
  }

  @Test
  void shouldNotDeleteWithRepositoryError()
  {
    ObjectNode id = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1)));

    when(identityParser.parse(id.get(FIELD_ID).asText())).thenReturn(id);
    doThrow(new DeleteException("TESTING")).when(dataRepository).delete(id);

    ResponseEntity<ObjectNode> response = testRestTemplate.exchange("/1", HttpMethod.DELETE, null, ObjectNode.class);

    assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
    assertNotNull(response.getBody());
    assertEquals("TESTING", response.getBody().get(FIELD_MESSAGE).asText());
  }

  private ObjectNode objectNode(Map<String, JsonNode> properties)
  {
    ObjectNode objectNode = new ObjectNode(null);
    objectNode.setAll(properties);

    return objectNode;
  }

  private HttpEntity<?> body(Object body)
  {
    return new HttpEntity<>(body, new LinkedMultiValueMap<>());
  }

  private <T> ArgumentMatcher<Iterable<T>> containsAll(Collection<T> elements)
  {
    return iterable -> StreamSupport.stream(iterable.spliterator(), false).allMatch(elements::contains);
  }

  private String encode(ObjectNode object) throws IOException
  {
    return Base64.getEncoder().encodeToString(objectMapper.writeValueAsBytes(object));
  }

  static class TestConfiguration
  {
    @Bean
    DataRepository<ObjectNode, ObjectNode> dataRepository()
    {
      //noinspection unchecked
      return mock(DataRepository.class);
    }

    @Bean
    IdentityParser<String, ObjectNode> idParser()
    {
      //noinspection unchecked
      return mock(IdentityParser.class);
    }

    @Bean
    JsonCodec<ObjectNode> jsonCodec(ObjectMapper objectMapper)
    {
      return new DefaultJsonCodec<>(objectMapper, ObjectNode.class);
    }

    @Bean
    QueryBuilder queryBuilder()
    {
      return new QueryBuilder(List.of(FIELD_ID, FIELD_NAME));
    }
  }
}
