package eu.altfive.playground;

import static java.lang.Math.abs;

import eu.altfive.playground.projection.model.ElasticModel;
import eu.altfive.playground.projection.model.ElasticModelNested;
import eu.altfive.playground.projection.model.ElasticModelNested.NestedSpecificAttribute;
import eu.altfive.playground.projection.query.ElasticsearchQuery;
import eu.altfive.playground.projection.query.SearchCriteria;
import eu.altfive.playground.projection.query.SearchCriteria.SpecificAttributeCriteria;
import eu.altfive.playground.projection.query.SearchCriteria.SpecificAttributeValueType;
import eu.altfive.playground.projection.repository.ElasticModelNestedRepository;
import eu.altfive.playground.projection.repository.ElasticModelRepository;
import java.io.IOException;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.jeasy.random.EasyRandom;
import org.jeasy.random.EasyRandomParameters;
import org.jeasy.random.api.Randomizer;
import org.jeasy.random.randomizers.AbstractRandomizer;
import org.jeasy.random.randomizers.number.DoubleRandomizer;
import org.jeasy.random.randomizers.range.DateRangeRandomizer;
import org.jeasy.random.randomizers.range.IntegerRangeRandomizer;
import org.jeasy.random.randomizers.text.StringRandomizer;
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
  private ElasticModelNestedRepository nestedRepository;

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

  protected class SpecificAttributesNestedRandomizer extends AbstractRandomizer<List<NestedSpecificAttribute>> {
    private final int seed;
    private final int nbElements;
    private final Randomizer<String> stringRandomizer;
    private final Randomizer<Integer> integerRandomizer;
    private final Randomizer<Double> doubleRandomizer;
    private final Randomizer<Date> dateRangeRandomizer;

    public SpecificAttributesNestedRandomizer() {
      seed = 123;
      nbElements = 10;
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
    public List<NestedSpecificAttribute> getRandomValue() {
      List<NestedSpecificAttribute> result = new ArrayList<>();
      for (int i = 0; i < nbElements; i++) {
//        Class<?> clazz = TYPES.get(random.nextInt(4));
        NestedSpecificAttribute attribute = new NestedSpecificAttribute();
        if (i == 0 || i == 1){
          attribute.setName("string-var-" + i);
          attribute.setValueString(stringRandomizer.getRandomValue());
        } else if (i == 2 || i == 3){
          attribute.setName("int-var-" + i);
          attribute.setValueLong(integerRandomizer.getRandomValue().longValue());
        } else if (i == 4 || i == 5){
          attribute.setName("double-var-" + i);
          attribute.setValueDouble(doubleRandomizer.getRandomValue());
        } else if (i == 6 || i == 7){
          attribute.setName("date-var-" + i);
          attribute.setValueDate(dateRangeRandomizer.getRandomValue());
        } else {
          attribute.setName("string-var-" + i);
          attribute.setValueString(stringRandomizer.getRandomValue());
        }
      }
      return result;
    }

  }


  protected class SpecificAttributesMapRandomizer extends AbstractRandomizer<Map<String, Serializable>> {
    private final int seed;
    private final int nbElements;
    private final Randomizer<String> stringRandomizer;
    private final Randomizer<Integer> integerRandomizer;
    private final Randomizer<Double> doubleRandomizer;
    private final Randomizer<Date> dateRangeRandomizer;

    public SpecificAttributesMapRandomizer() {
      seed = 123;
      nbElements = 10;
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
        } else {
          result.put("string-var-" + i, stringRandomizer.getRandomValue());
        }
      }
      return result;
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
    template.bulkIndex(list, ElasticModel.class);
  }

  private void loadENestedS(int number){
    EasyRandomParameters parameters = new EasyRandomParameters()
        .randomize(field -> field.getName().equals("processVariables"), new SpecificAttributesNestedRandomizer())
        .collectionSizeRange(10, 10);
    EasyRandom easyRandom = new EasyRandom(parameters);
    List<IndexQuery> list = easyRandom.objects(ElasticModelNested.class, number)
        .map(elasticModel -> new IndexQueryBuilder()
            .withIndex("model-nested")
            .withId(elasticModel.getId())
            .withObject(elasticModel)
            .build())
        .toList();
    template.bulkIndex(list, ElasticModelNested.class);
  }

  @Test
  void testSearchSpecificAttributesDatesBetween(){
    long time = System.currentTimeMillis();
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
    long time = System.currentTimeMillis();
    loadES(1000);
    loadES(1000);
    loadES(1000);
    loadES(1000);
    loadES(1000);
    loadES(1000);
    loadES(1000);
    loadES(1000);
    loadES(1000);
    loadES(1000);
    System.out.println("indexing time:"+(System.currentTimeMillis() - time));
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


  @Test
  void testSearchSpecificAttributesNumbersNested(){
    long time = System.currentTimeMillis();
    loadENestedS(1000);
    loadENestedS(1000);
    loadENestedS(1000);
    loadENestedS(1000);
    loadENestedS(1000);
    loadENestedS(1000);
    loadENestedS(1000);
    loadENestedS(1000);
    loadENestedS(1000);
    loadENestedS(1000);
    System.out.println("indexing time:"+(System.currentTimeMillis() - time));

//    System.out.println("count:"+repository.count());

    Page<SearchHit<ElasticModelNested>> search = elasticsearchQuery.searchNested(
        new SearchCriteria(List.of(
            new SpecificAttributeCriteria(
                "string-var", SpecificAttributeValueType.STRING, "toto", null, null, null,
                null, null
            )
        )), 10, 0
    );
    Assertions.assertTrue(search.getTotalElements() > 1);
  }

  @Test
  void testMultipleUpdatesToNestedDocument(){
    long start = System.currentTimeMillis();
    loadENestedS(100000);
    loadENestedS(100000);
    loadENestedS(100000);
    loadENestedS(100000);
    loadENestedS(100000);
    loadENestedS(100000);
    loadENestedS(100000);
    loadENestedS(100000);
    loadENestedS(100000);
    loadENestedS(100000);
    System.out.println("bulk time:"+(System.currentTimeMillis() - start));
    ElasticModelNested elasticModelNested = new ElasticModelNested();
    elasticModelNested.setId("1");
    List<NestedSpecificAttribute> nestedSpecificAttributes = new ArrayList<>();
    elasticModelNested.setProcessVariables(nestedSpecificAttributes);
    for (int i=0;i<1000;i++){
      NestedSpecificAttribute nestedSpecificAttribute = new NestedSpecificAttribute();
      nestedSpecificAttribute.setName("var-"+i);
      nestedSpecificAttribute.setValueString("value-"+i);
      nestedSpecificAttributes.add(nestedSpecificAttribute);
    }
    template.index(new IndexQueryBuilder()
            .withIndex("model-nested")
            .withId("1")
        .withObject(elasticModelNested)
        .build(), IndexCoordinates.of("model-nested"));
    template.indexOps(ElasticModelNested.class).refresh();

    long time = System.currentTimeMillis();
    for (int i=0;i<100;i++){
      elasticModelNested = nestedRepository.findById("1").orElseThrow();
      elasticModelNested.setName("name:"+i);
      elasticModelNested.getProcessVariables().get(0).setValueString("value-0");
      nestedRepository.save(elasticModelNested, RefreshPolicy.IMMEDIATE);
//      template.index(new IndexQueryBuilder()
//              .withId("1")
//          .withIndex("model-nested")
//          .withObject(elasticModelNested)
//          .build(), IndexCoordinates.of("model-nested"));
//      template.indexOps(ElasticModelNested.class).refresh();
    }

    System.out.println("indexing single object time:"+(System.currentTimeMillis() - time));
  }

  @Test
  void testMultipleUpdatesToNonNestedDocument(){
    long start = System.currentTimeMillis();
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
    System.out.println("bulk time:"+(System.currentTimeMillis() - start));
    ElasticModel elasticModel = new ElasticModel();
    elasticModel.setProcessVariables(new HashMap<>());
    elasticModel.setId("1");
    for (int i=0;i<1000;i++){
      elasticModel.getProcessVariables().put("var-"+i, "value-"+i);
    }
    template.index(new IndexQueryBuilder()
        .withIndex("model")
        .withId("1")
        .withObject(elasticModel)
        .build(), IndexCoordinates.of("model"));
    template.indexOps(ElasticModel.class).refresh();

    long time = System.currentTimeMillis();
    for (int i=0;i<100;i++){
      elasticModel = repository.findById("1").orElseThrow();
      elasticModel.getProcessVariables().put("var-1", "value-1");
      repository.save(elasticModel, RefreshPolicy.IMMEDIATE);
//      template.index(new IndexQueryBuilder()
//          .withId("1")
//          .withIndex("model")
//          .withObject(elasticModel)
//          .build(), IndexCoordinates.of("model"));
//      template.indexOps(ElasticModel.class).refresh();
    }

    System.out.println("indexing time:"+(System.currentTimeMillis() - time));
  }

}
