package com.tn.service.data.jdbc.api;

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

import java.io.IOException;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
import com.tn.service.data.io.KeyParser;
import com.tn.service.data.repository.DataRepository;
import com.tn.service.data.repository.DeleteException;
import com.tn.service.data.repository.InsertException;
import com.tn.service.data.repository.UpdateException;

@SpringBootTest(
  webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
  classes = DataApiIntegrationTest.TestConfiguration.class,
  properties = "tn.data.value-class=com.fasterxml.jackson.databind.node.ObjectNode"
)
@SuppressWarnings("SpringBootApplicationProperties")
@EnableAutoConfiguration
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
class DataApiIntegrationTest
{
  private static final String FIELD_ID = "id";
  private static final String FIELD_ID_2 = "id2";
  private static final String FIELD_NAME = "name";
  private static final ParameterizedTypeReference<List<ObjectNode>> TYPE_REFERENCE_OBJECTS = new ParameterizedTypeReference<>() {};
  private static final ParameterizedTypeReference<Page<ObjectNode>> TYPE_REFERENCE_PAGE = new ParameterizedTypeReference<>() {};

  @Autowired
  TestRestTemplate testRestTemplate;

  @Autowired
  DataRepository<ObjectNode, ObjectNode> dataRepository;

  @Autowired
  KeyParser<ObjectNode> keyParser;

  @Autowired
  ObjectMapper objectMapper;

  @Test
  void shouldGet()
  {
    ObjectNode data1 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_NAME, TextNode.valueOf("Data 1")));
    ObjectNode data2 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(2), FIELD_NAME, TextNode.valueOf("Data 2")));

    when(dataRepository.findAll()).thenReturn(List.of(data1, data2));

    ResponseEntity<ArrayNode> response = testRestTemplate.getForEntity("/", ArrayNode.class);

    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertEquals(new ArrayNode(null, List.of(data1, data2)), response.getBody());
  }

  @Test
  void shouldGetWithKey()
  {
    ObjectNode key = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1)));
    ObjectNode data = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_NAME, TextNode.valueOf("Data 1")));

    when(keyParser.parse(key.get(FIELD_ID).asText())).thenReturn(key);
    when(dataRepository.find(key)).thenReturn(Optional.of(data));

    ResponseEntity<ObjectNode> response = testRestTemplate.getForEntity("/" + key.get(FIELD_ID).asText(), ObjectNode.class);

    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertEquals(data, response.getBody());
  }

  @Test
  void shouldGetWithSimpleKeys()
  {
    ObjectNode key1 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1)));
    ObjectNode key2 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(2)));
    ObjectNode data1 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_NAME, TextNode.valueOf("Data 1")));
    ObjectNode data2 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(2), FIELD_NAME, TextNode.valueOf("Data 2")));

    when(keyParser.parse(key1.get(FIELD_ID).asText())).thenReturn(key1);
    when(keyParser.parse(key2.get(FIELD_ID).asText())).thenReturn(key2);
    when(dataRepository.findAll(List.of(key1, key2))).thenReturn(List.of(data1, data2));

    String url = UriComponentsBuilder.fromPath("/")
      .queryParam("key", List.of(key1.get(FIELD_ID).asText(), key2.get(FIELD_ID).asText()))
      .encode()
      .toUriString();

    ResponseEntity<ArrayNode> response = testRestTemplate.getForEntity(url, ArrayNode.class);

    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertEquals(new ArrayNode(null, List.of(data1, data2)), response.getBody());
  }

  @Test
  void shouldGetWithComplexKey() throws Exception
  {
    ObjectNode key = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_ID_2, TextNode.valueOf("A")));
    ObjectNode data = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_ID_2, TextNode.valueOf("A"), FIELD_NAME, TextNode.valueOf("Data 1")));

    String encodedKey = encode(key);

    when(keyParser.parse(encodedKey)).thenReturn(key);
    when(dataRepository.find(key)).thenReturn(Optional.of(data));

    ResponseEntity<ObjectNode> response = testRestTemplate.getForEntity("/" + encodedKey, ObjectNode.class);

    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertEquals(data, response.getBody());
  }

  @Test
  void shouldGetWithComplexKeys() throws Exception
  {
    ObjectNode key1 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_ID_2, TextNode.valueOf("A")));
    ObjectNode key2 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(2), FIELD_ID_2, TextNode.valueOf("B")));
    ObjectNode data1 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_ID_2, TextNode.valueOf("A"), FIELD_NAME, TextNode.valueOf("Data 1")));
    ObjectNode data2 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(2), FIELD_ID_2, TextNode.valueOf("B"), FIELD_NAME, TextNode.valueOf("Data 2")));

    String encodedKey1 = encode(key1);
    String encodedKey2 = encode(key2);

    when(keyParser.parse(encodedKey1)).thenReturn(key1);
    when(keyParser.parse(encodedKey2)).thenReturn(key2);
    when(dataRepository.findAll(List.of(key1, key2))).thenReturn(List.of(data1, data2));

    String url = UriComponentsBuilder.fromPath("/")
      .queryParam("key", List.of(encodedKey1, encodedKey2))
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

    when(dataRepository.findFor(query)).thenReturn(List.of(data));

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

    Page<ObjectNode> page = new Page<>(List.of(data), pageNumber, 2, DEFAULT_PAGE_SIZE + 1);

    when(dataRepository.findFor(query, pageNumber, DEFAULT_PAGE_SIZE)).thenReturn(page);

    ResponseEntity<Page<ObjectNode>> response = testRestTemplate.exchange(
      "/?q=" + query + "&pageNumber=" + pageNumber,
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

    Page<ObjectNode> page = new Page<>(List.of(data), DEFAULT_PAGE_NUMBER, 2, pageSize + 1);

    when(dataRepository.findFor(query, DEFAULT_PAGE_NUMBER, pageSize)).thenReturn(page);

    ResponseEntity<Page<ObjectNode>> response = testRestTemplate.exchange(
      "/?q=" + query + "&pageSize=" + pageSize,
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

    Page<ObjectNode> page = new Page<>(List.of(data), pageNumber, 2, pageSize + 1);

    when(dataRepository.findFor(query, pageNumber, pageSize)).thenReturn(page);

    ResponseEntity<Page<ObjectNode>> response = testRestTemplate.exchange(
      "/?q=" + query + "&pageNumber=" + pageNumber + "&pageSize=" + pageSize,
      HttpMethod.GET,
      null,
      TYPE_REFERENCE_PAGE
    );

    assertTrue(response.getStatusCode().is2xxSuccessful());
    assertEquals(page, response.getBody());
  }

  @Test
  void shouldNotGetForUnknownKey()
  {
    ObjectNode key = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1)));

    when(keyParser.parse(key.get(FIELD_ID).asText())).thenReturn(key);
    when(dataRepository.find(key)).thenReturn(Optional.empty());

    ResponseEntity<ObjectNode> response = testRestTemplate.getForEntity("/" + key.get(FIELD_ID).asText(), ObjectNode.class);

    assertTrue(response.getStatusCode().isSameCodeAs(HttpStatus.NOT_FOUND));
  }

  @Test
  void shouldNotGetWithKeyAndQuery()
  {
    ResponseEntity<ObjectNode> response = testRestTemplate.getForEntity("/?key=1&q=x", ObjectNode.class);

    assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    assertNotNull(response.getBody());
    assertEquals("Query parameter not allowed with key(s)", response.getBody().get(FIELD_MESSAGE).asText());
  }

  @ParameterizedTest
  @ValueSource(strings = {"pageNumber=0", "pageSize=10", "pageNumber=0&pageSize=10"})
  void shouldNotGetWithKeyAndPagination(String paginationParameter)
  {
    ResponseEntity<ObjectNode> response = testRestTemplate.getForEntity("/?key=1&" + paginationParameter, ObjectNode.class);

    assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    assertNotNull(response.getBody());
    assertEquals("Pagination not supported with Key(s)", response.getBody().get(FIELD_MESSAGE).asText());
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

    ResponseEntity<List<ObjectNode>> response = testRestTemplate.exchange("/", HttpMethod.POST, body(data), TYPE_REFERENCE_OBJECTS);

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

    ResponseEntity<List<ObjectNode>> response = testRestTemplate.exchange("/", HttpMethod.PUT, body(data), TYPE_REFERENCE_OBJECTS);

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
  void shouldDeleteWithSimpleKey()
  {
    ObjectNode key = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1)));

    when(keyParser.parse(key.get(FIELD_ID).asText())).thenReturn(key);

    ResponseEntity<Void> response = testRestTemplate.exchange("/" + key.get(FIELD_ID).asText(), HttpMethod.DELETE, null, Void.class);

    assertTrue(response.getStatusCode().is2xxSuccessful());

    verify(dataRepository).delete(key);
  }

  @Test
  void shouldDeleteWithSimpleKeys()
  {
    ObjectNode key1 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1)));
    ObjectNode key2 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(2)));

    when(keyParser.parse(key1.get(FIELD_ID).asText())).thenReturn(key1);
    when(keyParser.parse(key2.get(FIELD_ID).asText())).thenReturn(key2);

    String url = UriComponentsBuilder.fromPath("/")
      .queryParam("key", List.of(key1.get(FIELD_ID).asText(), key2.get(FIELD_ID).asText()))
      .encode()
      .toUriString();

    ResponseEntity<Void> response = testRestTemplate.exchange(url, HttpMethod.DELETE, null, Void.class);

    assertTrue(response.getStatusCode().is2xxSuccessful());

    verify(dataRepository).deleteAll(List.of(key1, key2));
  }

  @Test
  void shouldDeleteWithComplexKey() throws Exception
  {
    ObjectNode key = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_ID_2, TextNode.valueOf("A")));

    String encodedKey = encode(key);

    when(keyParser.parse(encodedKey)).thenReturn(key);

    ResponseEntity<Void> response = testRestTemplate.exchange("/" + encodedKey, HttpMethod.DELETE, null, Void.class);

    assertTrue(response.getStatusCode().is2xxSuccessful());

    verify(dataRepository).delete(key);
  }

  @Test
  void shouldDeleteWithComplexKeys() throws Exception
  {
    ObjectNode key1 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1), FIELD_ID_2, TextNode.valueOf("A")));
    ObjectNode key2 = objectNode(Map.of(FIELD_ID, IntNode.valueOf(2), FIELD_ID_2, TextNode.valueOf("B")));

    String encodedKey1 = encode(key1);
    String encodedKey2 = encode(key2);

    when(keyParser.parse(encodedKey1)).thenReturn(key1);
    when(keyParser.parse(encodedKey2)).thenReturn(key2);

    String url = UriComponentsBuilder.fromPath("/")
      .queryParam("key", List.of(encodedKey1, encodedKey2))
      .encode()
      .toUriString();

    ResponseEntity<ArrayNode> response = testRestTemplate.exchange(url, HttpMethod.DELETE, null, ArrayNode.class);

    assertTrue(response.getStatusCode().is2xxSuccessful());

    verify(dataRepository).deleteAll(List.of(key1, key2));
  }

  @Test
  void shouldNotDeleteWithRepositoryError()
  {
    ObjectNode key = objectNode(Map.of(FIELD_ID, IntNode.valueOf(1)));

    when(keyParser.parse(key.get(FIELD_ID).asText())).thenReturn(key);
    doThrow(new DeleteException("TESTING")).when(dataRepository).delete(key);

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
    KeyParser<ObjectNode> keyParser()
    {
      //noinspection unchecked
      return mock(KeyParser.class);
    }
  }
}
