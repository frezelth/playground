package eu.altfive.playground;

import static java.lang.Math.abs;

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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.jeasy.random.EasyRandom;
import org.jeasy.random.EasyRandomParameters;
import org.jeasy.random.api.Randomizer;
import org.jeasy.random.randomizers.AbstractRandomizer;
import org.jeasy.random.randomizers.collection.MapRandomizer;
import org.jeasy.random.randomizers.number.ByteRandomizer;
import org.jeasy.random.randomizers.number.DoubleRandomizer;
import org.jeasy.random.randomizers.number.IntegerRandomizer;
import org.jeasy.random.randomizers.range.DateRangeRandomizer;
import org.jeasy.random.randomizers.range.IntegerRangeRandomizer;
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
import org.springframework.data.elasticsearch.core.RefreshPolicy;
import org.springframework.data.elasticsearch.core.SearchHit;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.BulkOptions;
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

  protected class SpecificAttributesMapRandomizer extends AbstractRandomizer<Map<String, Serializable>> {
    private final int seed;
    private final int nbElements;

//    private final static List<Class<?>> TYPES = List.of(
//        String.class,
//        Integer.class,
//        Date.class,
//        Double.class
//    );

    private final Randomizer<String> stringRandomizer;
    private final Randomizer<Integer> integerRandomizer;
    private final Randomizer<Double> doubleRandomizer;
    private final Randomizer<Date> dateRangeRandomizer;

    public SpecificAttributesMapRandomizer() {
      seed = 123;
      nbElements = 8;
      stringRandomizer = new StringRandomizer(seed);
      integerRandomizer = new IntegerRangeRandomizer(0, 1000);
      doubleRandomizer = new DoubleRandomizer(seed);
      dateRangeRandomizer = new DateRangeRandomizer(
          new Date(LocalDate.of(2023,11,1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli()),
          new Date(LocalDate.of(2023,12,31).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli()),
          seed
      );
    }

    @Override
    public Map<String, Serializable> getRandomValue() {
      Map<String, Serializable> result = new HashMap<>();
      for (int i = 0; i < nbElements; i++) {
//        Class<?> clazz = TYPES.get(random.nextInt(4));
        if (i == 0 || i == 1){
          result.put("string-var-" + i, stringRandomizer.getRandomValue());
        } else if (i == 2 || i == 3){
          result.put("int-var-" + i, integerRandomizer.getRandomValue());
        } else if (i == 4 || i == 5){
          result.put("double-var-" + i, doubleRandomizer.getRandomValue());
        } else if (i == 6 || i == 7){
          result.put("date-var-" + i, dateRangeRandomizer.getRandomValue());
        }
      }
      return result;
    }

    private void checkArguments(final int nbEntries) {
      if (nbEntries < 0) {
        throw new IllegalArgumentException("The number of entries to generate must be >= 0");
      }
    }

    private static int getRandomSize() {
      return abs(new ByteRandomizer().getRandomValue()) + 1;
    }
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
        return new IntegerRangeRandomizer(0, 1000).getRandomValue();
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

  private void loadES(int number){
    EasyRandomParameters parameters = new EasyRandomParameters()
        .randomize(field -> field.getName().equals("processVariables"), new SpecificAttributesMapRandomizer());
    EasyRandom easyRandom = new EasyRandom(parameters);
    List<IndexQuery> list = easyRandom.objects(ElasticModel.class, number)
        .map(elasticModel -> new IndexQueryBuilder()
            .withIndex("model")
            .withId(elasticModel.getId())
            .withObject(elasticModel)
            .build())
        .toList();
    template.bulkIndex(list,
        BulkOptions.builder().withRefreshPolicy(RefreshPolicy.IMMEDIATE).build(), ElasticModel.class);
  }

  @Test
  void testSearchSpecificAttributesDatesBetween(){
    loadES(100000);
    loadES(100000);
    loadES(100000);
    loadES(100000);
    loadES(100000);
    loadES(100000);
    loadES(100000);
    loadES(100000);
    loadES(100000);
    loadES(100000);
    System.out.println("count:"+repository.count());

    Page<SearchHit<ElasticModel>> search = elasticsearchQuery.search(
        new SearchCriteria(List.of(
            new SpecificAttributeCriteria(
                "date-var-6", SpecificAttributeValueType.DATE, null, null, null, null,
                LocalDate.of(2023,12,15).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli(), LocalDate.of(2023,12,1).atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli()
            )
        )), 10, 0
    );
    Assertions.assertTrue(search.getTotalElements() > 1);
  }

  @Test
  void testSearchSpecificAttributesNumbersBetween(){
    loadES(100000);
    loadES(100000);
    loadES(100000);
    loadES(100000);
    loadES(100000);
    loadES(100000);
    loadES(100000);
    loadES(100000);
    loadES(100000);
    loadES(100000);

    System.out.println("count:"+repository.count());

    Page<SearchHit<ElasticModel>> search = elasticsearchQuery.search(
        new SearchCriteria(List.of(
            new SpecificAttributeCriteria(
                "int-var-2", SpecificAttributeValueType.INTEGER, null, null, 500.0, 1.0,
                null, null
            )
        )), 10, 0
    );
    Assertions.assertTrue(search.getTotalElements() > 1);
  }

}
