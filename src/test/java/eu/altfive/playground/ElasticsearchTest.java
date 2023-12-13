package eu.altfive.playground;

import eu.altfive.playground.model.ElasticModel;
import eu.altfive.playground.query.ElasticsearchQuery;
import eu.altfive.playground.query.SearchCriteria;
import eu.altfive.playground.query.SearchCriteria.SpecificAttributeCriteria;
import eu.altfive.playground.query.SearchCriteria.SpecificAttributeValueType;
import eu.altfive.playground.repository.ElasticModelRepository;
import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.jeasy.random.EasyRandom;
import org.jeasy.random.EasyRandomParameters;
import org.jeasy.random.api.Randomizer;
import org.jeasy.random.randomizers.AbstractRandomizer;
import org.jeasy.random.randomizers.number.DoubleRandomizer;
import org.jeasy.random.randomizers.number.IntegerRandomizer;
import org.jeasy.random.randomizers.range.DateRangeRandomizer;
import org.jeasy.random.randomizers.text.StringRandomizer;
import org.jeasy.random.randomizers.time.DateRandomizer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.domain.Page;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;
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

  @Autowired
  private ElasticsearchTemplate template;

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

    long before = System.currentTimeMillis();
    ElasticModel retrieved = repository.findById("1").orElseThrow();
    System.out.println("TIME:"+(System.currentTimeMillis() - before));
    org.assertj.core.api.Assertions.assertThat(retrieved).usingRecursiveComparison().isEqualTo(saved);
  }

  @AfterEach
  void removeAllData(){
    repository.deleteAll();
  }

  @Test
  void testSearchSpecificAttributesStringEq(){
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

  protected class SpecificAttributesRandomizer extends AbstractRandomizer<Serializable> {

    private final static List<Class<?>> TYPES = List.of(
      String.class,
      Integer.class,
      Date.class,
      Double.class
    );

    @Override
    public Serializable getRandomValue() {
      Class<?> clazz = TYPES.get(random.nextInt(4));
      if (clazz == String.class){
        return new StringRandomizer(10).getRandomValue();
      } else if (clazz == Integer.class){
        return new IntegerRandomizer().getRandomValue();
      } else if (clazz == Double.class){
        return new DoubleRandomizer().getRandomValue();
      } else if (clazz == Date.class){
        return new DateRangeRandomizer(
            new Date(LocalDate.of(2023,11,1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli()),
            new Date(LocalDate.of(2023,12,31).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli())
        ).getRandomValue();
      }
      return null;
    }
  }

  private List<IndexQuery> loadES(int number){
    EasyRandomParameters parameters = new EasyRandomParameters()
        .randomize(Serializable.class, new SpecificAttributesRandomizer());
    EasyRandom easyRandom = new EasyRandom(parameters);
    return easyRandom.objects(ElasticModel.class, number)
        .map(elasticModel -> new IndexQueryBuilder()
            .withIndex("model")
            .withId(elasticModel.getId())
            .withObject(elasticModel)
            .build())
        .toList();
  }

  @Test
  void testSearchSpecificAttributesDatesBetween(){

    List<IndexQuery> indexQueries = loadES(1000);
    template.bulkIndex(indexQueries, ElasticModel.class);
    template.indexOps(ElasticModel.class).refresh();
    System.out.println("count:"+repository.count());

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
                "key-date", SpecificAttributeValueType.DATE, null, null, null, null,
                LocalDate.of(2023,12,15).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli(), LocalDate.of(2023,12,1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli()
            )
        )), 10, 0
    );
    Assertions.assertEquals(1, search.getTotalElements());
  }

  @Test
  void loadTest(){
    List<IndexQuery> indexQueries = loadES(100);
    template.bulkIndex(indexQueries, ElasticModel.class);
    template.indexOps(ElasticModel.class).refresh();
    System.out.println("count:"+repository.count());
  }
}
