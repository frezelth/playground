package eu.altfive.playground;

import eu.altfive.playground.command.AddVariable;
import eu.altfive.playground.command.CreateModel;
import eu.altfive.playground.command.SetParent;
import eu.altfive.playground.command.VariableValue;
import eu.altfive.playground.projection.model.ElasticModelNested.NestedSpecificAttribute;
import java.io.Serializable;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.jeasy.random.api.Randomizer;
import org.jeasy.random.randomizers.AbstractRandomizer;
import org.jeasy.random.randomizers.number.DoubleRandomizer;
import org.jeasy.random.randomizers.range.DateRangeRandomizer;
import org.jeasy.random.randomizers.range.DoubleRangeRandomizer;
import org.jeasy.random.randomizers.range.IntegerRangeRandomizer;
import org.jeasy.random.randomizers.range.LongRangeRandomizer;
import org.jeasy.random.randomizers.text.StringRandomizer;
import org.jeasy.random.randomizers.time.DateRandomizer;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@Component
public class Load {

  private final BrokerSimulator brokerSimulator;

  private static final int TOTAL_NUMBER_OF_AGGREGATES = 10000;

  private final Random random = new Random();
  private final StringRandomizer stringRandomizer = new StringRandomizer();
  private final LongRangeRandomizer longRangeRandomizer = new LongRangeRandomizer(0L, 1000L);
  private final DoubleRangeRandomizer doubleRandomizer = new DoubleRangeRandomizer(0.0, 1000.0);
  private final DateRandomizer dateRandomizer = new DateRandomizer();

  public Load(BrokerSimulator brokerSimulator) {
    this.brokerSimulator = brokerSimulator;
  }

  @EventListener
  public void onApplicationStart(ApplicationReadyEvent event){

    List<String> aggregateIds = new ArrayList<>();
    // list with all aggregates
    for (int i=0; i<TOTAL_NUMBER_OF_AGGREGATES; i++) {
      aggregateIds.add(UUID.randomUUID().toString());
    }

    // 50% = top processes
    // 20% = first level children
    // 15% = second level children
    // 10% = third level children
    // 5% = fourth level children
    List<String> firstChildren = aggregateIds.subList(0, (int)Math.round(aggregateIds.size() * 0.2));
    List<String> secondChildren = aggregateIds.subList(firstChildren.size(), firstChildren.size() + (int)Math.round(aggregateIds.size() * 0.15));
    List<String> thirdChildren = aggregateIds.subList(firstChildren.size() + secondChildren.size(), firstChildren.size() + secondChildren.size() + (int)Math.round(aggregateIds.size() * 0.1));
    List<String> fourthChildren = aggregateIds.subList(firstChildren.size() + secondChildren.size() + thirdChildren.size(), firstChildren.size() + secondChildren.size() + thirdChildren.size() + (int)Math.round(aggregateIds.size() * 0.05));

    List<String> rootLevel = aggregateIds.subList(firstChildren.size() + secondChildren.size() + thirdChildren.size() + fourthChildren.size(),
        aggregateIds.size());

    Map<String,String> childToParent = new HashMap<>();

    fourthChildren.forEach(s -> childToParent.put(s, thirdChildren.get(random.nextInt(
        thirdChildren.size()))));
    thirdChildren.forEach(s -> childToParent.put(s, secondChildren.get(random.nextInt(
        secondChildren.size()))));
    secondChildren.forEach(s -> childToParent.put(s, firstChildren.get(random.nextInt(
        firstChildren.size()))));
    firstChildren.forEach(s -> childToParent.put(s, rootLevel.get(random.nextInt(rootLevel.size()))));

    for (String aggregateId : aggregateIds) {
      brokerSimulator.sendCommand(aggregateId, new CreateModel(aggregateId, "name_"+aggregateId));
      String parentId = childToParent.get(aggregateId);
      if (parentId != null){
        brokerSimulator.sendCommand(aggregateId, new SetParent(aggregateId, parentId));
      }

      for (int i=0;i<5;i++){
        int r = random.nextInt(4);
        if (r == 0){
          brokerSimulator.sendCommand(aggregateId, new AddVariable(aggregateId,
              UUID.randomUUID().toString(), new VariableValue(stringRandomizer.getRandomValue(), null, null, null)));
        } else if (r == 1){
          brokerSimulator.sendCommand(aggregateId, new AddVariable(aggregateId, UUID.randomUUID().toString(), new VariableValue(null, longRangeRandomizer.getRandomValue(), null, null)));
        } else if (r == 2){
          brokerSimulator.sendCommand(aggregateId, new AddVariable(aggregateId, UUID.randomUUID().toString(), new VariableValue(null, null,
              doubleRandomizer.getRandomValue(), null)));
        } else {
          brokerSimulator.sendCommand(aggregateId, new AddVariable(aggregateId, UUID.randomUUID().toString(), new VariableValue(null, null,
              null, dateRandomizer.getRandomValue())));
        }
      }

    }


//    for (String aggregateId : aggregateIds) {
//      commandGateway.sendAndWait(new CreateModel(aggregateId, "name_"+aggregateId));
//      String parentId = childToParent.get(aggregateId);
//      if (parentId != null){
//        commandGateway.sendAndWait(new SetParent(aggregateId, parentId));
//      }
//
//      for (int i=0;i<5;i++){
//        int r = random.nextInt(4);
//        if (r == 0){
//          commandGateway.sendAndWait(new AddVariable(aggregateId,
//              UUID.randomUUID().toString(), new VariableValue(stringRandomizer.getRandomValue(), null, null, null)));
//        } else if (r == 1){
//          commandGateway.sendAndWait(new AddVariable(aggregateId, UUID.randomUUID().toString(), new VariableValue(null, longRangeRandomizer.getRandomValue(), null, null)));
//        } else if (r == 2){
//          commandGateway.sendAndWait(new AddVariable(aggregateId, UUID.randomUUID().toString(), new VariableValue(null, null,
//              doubleRandomizer.getRandomValue(), null)));
//        } else {
//          commandGateway.sendAndWait(new AddVariable(aggregateId, UUID.randomUUID().toString(), new VariableValue(null, null,
//              null, dateRandomizer.getRandomValue())));
//        }
//      }
//
//    }

//
//    EasyRandomParameters parameters = new EasyRandomParameters();
//    EasyRandom easyRandom = new EasyRandom(parameters);
//    List<String> aggregateIds = new ArrayList<>();
//
//    for (int i=0;i<1000;i++){
//      String id = UUID.randomUUID().toString();
//      commandGateway.sendAndWait(new CreateModel(id, "name_"+i), MetaData.with("ancestor", id));
//      aggregateIds.add(id);
//    }
//
//    List<String> firstPart = aggregateIds.subList(0,200);
//    List<String> secondPart = aggregateIds.subList(200,400);
//    List<String> thirdPart = aggregateIds.subList(400,600);
//    List<String> fourthPart = aggregateIds.subList(600,800);
//    List<String> fifthPart = aggregateIds.subList(800,1000);
//
//    Map<String, String> aggregateToAncestor = new HashMap<>();
//
//    for (int i=0;i<200;i++){
//      commandGateway.send(new SetParent(firstPart.get(i), secondPart.get(i), fifthPart.get(i)), MetaData.with("ancestor", fifthPart.get(i)));
//      commandGateway.send(new SetParent(secondPart.get(i), thirdPart.get(i), fifthPart.get(i)), MetaData.with("ancestor", fifthPart.get(i)));
//      commandGateway.send(new SetParent(thirdPart.get(i), fourthPart.get(i), fifthPart.get(i)), MetaData.with("ancestor", fifthPart.get(i)));
//      commandGateway.send(new SetParent(fourthPart.get(i), fifthPart.get(i), fifthPart.get(i)), MetaData.with("ancestor", fifthPart.get(i)));
//
//      aggregateToAncestor.put(firstPart.get(i), fifthPart.get(i));
//      aggregateToAncestor.put(secondPart.get(i), fifthPart.get(i));
//      aggregateToAncestor.put(thirdPart.get(i), fifthPart.get(i));
//      aggregateToAncestor.put(fourthPart.get(i), fifthPart.get(i));
//    }
//
  }


  protected class SpecificAttributesNestedRandomizer extends
      AbstractRandomizer<List<NestedSpecificAttribute>> {
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

//  private void loadES(int number){
//    EasyRandomParameters parameters = new EasyRandomParameters()
//        .randomize(field -> field.getName().equals("processVariables"), new SpecificAttributesMapRandomizer());
//    EasyRandom easyRandom = new EasyRandom(parameters);
//    List<IndexQuery> list = easyRandom.objects(ElasticModel.class, number)
//        .map(elasticModel -> new IndexQueryBuilder()
//            .withIndex("model")
//            .withId(elasticModel.getId())
//            .withObject(elasticModel)
//            .build())
//        .toList();
//    template.bulkIndex(list, ElasticModel.class);
//  }
//
//  private void loadENestedS(int number){
//    EasyRandomParameters parameters = new EasyRandomParameters()
//        .randomize(field -> field.getName().equals("processVariables"), new SpecificAttributesNestedRandomizer())
//        .collectionSizeRange(10, 10);
//    EasyRandom easyRandom = new EasyRandom(parameters);
//    List<IndexQuery> list = easyRandom.objects(ElasticModelNested.class, number)
//        .map(elasticModel -> new IndexQueryBuilder()
//            .withIndex("model-nested")
//            .withId(elasticModel.getId())
//            .withObject(elasticModel)
//            .build())
//        .toList();
//    template.bulkIndex(list, ElasticModelNested.class);
//  }

}
