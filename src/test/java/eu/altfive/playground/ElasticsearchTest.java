package eu.altfive.playground;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.indices.GetIndicesSettingsRequest;
import co.elastic.clients.elasticsearch.indices.GetIndicesSettingsResponse;
import co.elastic.clients.elasticsearch.indices.GetMappingRequest;
import co.elastic.clients.elasticsearch.indices.GetMappingResponse;
import co.elastic.clients.transport.rest_client.RestClientHttpClient;
import eu.altfive.playground.model.ElasticModel;
import eu.altfive.playground.model.SpecificAttributes;
import eu.altfive.playground.query.ElasticsearchQuery;
import eu.altfive.playground.query.SearchCriteria;
import eu.altfive.playground.query.SearchCriteria.SpecificAttributeCriteria;
import eu.altfive.playground.query.SearchCriteria.SpecificAttributeValueType;
import eu.altfive.playground.repository.ElasticModelRepository;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.domain.Page;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@SpringBootTest
@Testcontainers
class ElasticsearchTest {

  @Autowired
  private ElasticModelRepository repository;

  @Autowired
  private ElasticsearchQuery elasticsearchQuery;

  @Container
  @ServiceConnection
  static ElasticsearchContainer elasticsearch = new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:7.17.10");


  @Test
  void testRetrieveData() throws IOException {
    ElasticModel saved = new ElasticModel();
    saved.setId("1");
    saved.setProcessVariables(
        Map.of(
            "key-string", "value-string",
            "key-date", new Date(),
            "key-int", 123
        )
    );
    repository.save(saved);

    ElasticModel retrieved = repository.findById("1").orElseThrow();
    org.assertj.core.api.Assertions.assertThat(retrieved).usingRecursiveComparison().isEqualTo(saved);
  }

  @AfterEach
  void removeAllData(){
    repository.deleteAll();
  }

  @Test
  void testSearchSpecificAttributes(){
    ElasticModel saved = new ElasticModel();
    saved.setId("1");
    saved.setProcessVariables(
        Map.of(
            "key-string", "value-string",
            "key-date", new Date(),
            "key-int", 123
        )
    );
    repository.save(saved);
    Page<SearchHit<ElasticModel>> search = elasticsearchQuery.search(
        new SearchCriteria(List.of(
            new SpecificAttributeCriteria(
                "key-string", SpecificAttributeValueType.STRING, "value-string", null, null, null,
                null, null
            )
        )), 10, 0
    );
    Assertions.assertEquals(1, search.getTotalElements());
  }

}
