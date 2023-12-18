package eu.altfive.playground.projection;

import eu.altfive.playground.command.VariableValue;
import eu.altfive.playground.event.ModelCreated;
import eu.altfive.playground.event.VariableAdded;
import eu.altfive.playground.event.VariableUpdated;
import eu.altfive.playground.projection.model.ElasticModel;
import eu.altfive.playground.projection.model.ElasticModelNested;
import eu.altfive.playground.projection.model.ElasticModelNested.NestedSpecificAttribute;
import eu.altfive.playground.projection.repository.ElasticModelNestedRepository;
import eu.altfive.playground.projection.repository.ElasticModelRepository;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.messaging.unitofwork.UnitOfWork;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.RefreshPolicy;
import org.springframework.data.elasticsearch.core.query.BulkOptions;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@ProcessingGroup("elastic-nested")
@Component
public class ElasticNestedEventHandler {

  private final ElasticModelNestedRepository repository;

  private static final AtomicLong startTime = new AtomicLong(0);
  private static final AtomicLong ongoingTime = new AtomicLong(0);
  private static final AtomicLong lastCheckTime = new AtomicLong(0);
  private final ElasticsearchTemplate elasticsearchTemplate;

  public ElasticNestedEventHandler(ElasticModelNestedRepository repository, ElasticsearchTemplate elasticsearchTemplate) {
    this.repository = repository;
    this.elasticsearchTemplate = elasticsearchTemplate;
  }

  @Scheduled(initialDelay = 10000, fixedDelay = 5000)
  public void checkChanges(){
    if (lastCheckTime.longValue() == ongoingTime.longValue()){
      System.out.println("finished ES nested model indexing:"+(ongoingTime.longValue() - startTime.longValue()));
    }
    lastCheckTime.set(ongoingTime.longValue());
  }

  @EventHandler
  void handle(ModelCreated event, UnitOfWork<?> unitOfWork){
    if (startTime.longValue() == 0){
      startTime.set(System.currentTimeMillis());
    }
//    ElasticModelNested model = getOngoingBatchRecord(unitOfWork, event.id(), true);
    ElasticModelNested model = new ElasticModelNested();
    model.setId(event.id());
    model.setName(event.name());
    repository.save(model);
    ongoingTime.set(System.currentTimeMillis());
  }

  @EventHandler
  void handle(VariableAdded event, UnitOfWork<?> unitOfWork){
    ElasticModelNested model = repository.findById(event.id()).orElseThrow();
//    ElasticModelNested model = getOngoingBatchRecord(unitOfWork, event.id(), false);
    if (model.getProcessVariables() == null){
      model.setProcessVariables(new ArrayList<>());
    }
    NestedSpecificAttribute nestedSpecificAttribute = new NestedSpecificAttribute();
    nestedSpecificAttribute.setName(event.name());

    if (event.value().stringValue() != null){
      nestedSpecificAttribute.setValueString(event.value().stringValue());
    } else if (event.value().longValue() != null){
      nestedSpecificAttribute.setValueLong(event.value().longValue());
    } else if (event.value().doubleValue() != null){
      nestedSpecificAttribute.setValueDouble(event.value().doubleValue());
    } else if (event.value().dateValue() != null){
      nestedSpecificAttribute.setValueDate(event.value().dateValue());
    } else {
      throw new IllegalArgumentException();
    }
    model.getProcessVariables().add(nestedSpecificAttribute);
    repository.save(model);
    ongoingTime.set(System.currentTimeMillis());
  }

  @EventHandler
  void handle(VariableUpdated event, UnitOfWork<?> unitOfWork){
    ElasticModelNested model = repository.findById(event.id()).orElseThrow();
//    ElasticModelNested model = getOngoingBatchRecord(unitOfWork, event.id(), false);
    NestedSpecificAttribute nestedSpecificAttribute = model.getProcessVariables().stream()
        .filter(s -> s.getName().equals(event.name()))
        .findFirst()
        .orElseThrow();
    nestedSpecificAttribute.setValueDate(null);
    nestedSpecificAttribute.setValueDouble(null);
    nestedSpecificAttribute.setValueLong(null);
    nestedSpecificAttribute.setValueDate(null);
    if (event.value().stringValue() != null){
      nestedSpecificAttribute.setValueString(event.value().stringValue());
    } else if (event.value().longValue() != null){
      nestedSpecificAttribute.setValueLong(event.value().longValue());
    } else if (event.value().doubleValue() != null){
      nestedSpecificAttribute.setValueDouble(event.value().doubleValue());
    } else if (event.value().dateValue() != null){
      nestedSpecificAttribute.setValueDate(event.value().dateValue());
    } else {
      throw new IllegalArgumentException();
    }
    repository.save(model);
    ongoingTime.set(System.currentTimeMillis());
  }

  private ElasticModelNested getOngoingBatchRecord(UnitOfWork<?> unitOfWork, String aggregateId,
      boolean create){
    Map<String, ElasticModelNested> currentBatch = unitOfWork.getOrComputeResource(
        "current-es-batch", k -> {
          Map<String, ElasticModelNested> map = new HashMap<>();
          unitOfWork.onPrepareCommit(uow -> {
            List<IndexQuery> list = map.values()
                .stream().map(elasticModel -> new IndexQueryBuilder()
                    .withIndex("model-nested")
                    .withId(elasticModel.getId())
                    .withObject(elasticModel)
                    .build())
                .toList();
            elasticsearchTemplate.bulkIndex(list, BulkOptions.builder().withRefreshPolicy(
                RefreshPolicy.IMMEDIATE).build(), ElasticModelNested.class);
          });
          return map;
        });
    if (create){
      currentBatch.put(aggregateId, new ElasticModelNested());
    } else if (currentBatch.get(aggregateId) == null){
      currentBatch.put(aggregateId, repository.findById(aggregateId).orElseThrow());
    }
    return currentBatch.get(aggregateId);
  }

}
