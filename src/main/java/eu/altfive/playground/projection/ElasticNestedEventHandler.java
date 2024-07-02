package eu.altfive.playground.projection;

import eu.altfive.playground.BrokerSimulator;
import eu.altfive.playground.command.AddVariable;
import eu.altfive.playground.command.VariableValue;
import eu.altfive.playground.event.ModelCreated;
import eu.altfive.playground.event.ParentSet;
import eu.altfive.playground.event.VariableAdded;
import eu.altfive.playground.event.VariableUpdated;
import eu.altfive.playground.projection.model.ElasticModel;
import eu.altfive.playground.projection.model.ElasticModelNested;
import eu.altfive.playground.projection.model.ElasticModelNested.NestedSpecificAttribute;
import eu.altfive.playground.projection.repository.ElasticModelNestedRepository;
import eu.altfive.playground.projection.repository.ElasticModelRepository;
import io.micrometer.common.util.StringUtils;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.messaging.unitofwork.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.RefreshPolicy;
import org.springframework.data.elasticsearch.core.query.BulkOptions;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

//@ProcessingGroup("elastic-nested")
//@Component
public class ElasticNestedEventHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticNestedEventHandler.class);
  private final ElasticModelNestedRepository repository;
  private final BrokerSimulator brokerSimulator;

  private static final AtomicLong startTime = new AtomicLong(0);
  private static final AtomicLong ongoingTime = new AtomicLong(0);
  private static final AtomicLong lastCheckTime = new AtomicLong(0);
  private final ElasticsearchTemplate elasticsearchTemplate;
  private final CommandGateway commandGateway;

  public ElasticNestedEventHandler(ElasticModelNestedRepository repository,
      ElasticsearchTemplate elasticsearchTemplate,
      BrokerSimulator brokerSimulator,
      CommandGateway commandGateway) {
    this.repository = repository;
    this.elasticsearchTemplate = elasticsearchTemplate;
    this.brokerSimulator = brokerSimulator;
    this.commandGateway = commandGateway;
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
    ElasticModelNested model = getOngoingBatchRecord(unitOfWork, event.id(), true);
//    ElasticModelNested model = new ElasticModelNested();
    model.setId(event.id());
    model.setName(event.name());
//    model.setVersion(model.getVersion() + 1);
//    repository.save(model);
    ongoingTime.set(System.currentTimeMillis());
  }

  @EventHandler
  void handle(VariableAdded event, UnitOfWork<?> unitOfWork){
//    ElasticModelNested model = repository.findById(event.id()).orElseThrow();
//    ElasticModelNested model = getOngoingBatchRecord(unitOfWork, event.id(), false);
    handleVariableAdded(event.id(), event.name(), event.value(), unitOfWork);
//    repository.save(model);
    ongoingTime.set(System.currentTimeMillis());
  }

  @EventHandler
  void handle(VariableUpdated event, UnitOfWork<?> unitOfWork){
//    ElasticModelNested model = repository.findById(event.id()).orElseThrow();
//    handleVariableUpdated(event.id(), event.name(), event.value(), unitOfWork);
//    repository.save(model);
    ongoingTime.set(System.currentTimeMillis());
  }

  private void handleVariableAdded(String id, String name, VariableValue value, UnitOfWork<?> unitOfWork) {
    ElasticModelNested model = getOngoingBatchRecord(unitOfWork, id, false);
//    ElasticModelNested model = repository.findById(modelId).orElseThrow();
    if (model.getProcessVariables() == null){
      model.setProcessVariables(new ArrayList<>());
    }
    NestedSpecificAttribute nestedSpecificAttribute = new NestedSpecificAttribute();
    nestedSpecificAttribute.setName(name);
    nestedSpecificAttribute.setValueDate(null);
    nestedSpecificAttribute.setValueDouble(null);
    nestedSpecificAttribute.setValueLong(null);
    nestedSpecificAttribute.setValueDate(null);
    if (value.stringValue() != null){
      nestedSpecificAttribute.setValueString(value.stringValue());
    } else if (value.longValue() != null){
      nestedSpecificAttribute.setValueLong(value.longValue());
    } else if (value.doubleValue() != null){
      nestedSpecificAttribute.setValueDouble(value.doubleValue());
    } else if (value.dateValue() != null){
      nestedSpecificAttribute.setValueDate(value.dateValue());
    } else {
      throw new IllegalArgumentException();
    }
    model.getProcessVariables().add(nestedSpecificAttribute);
    if (StringUtils.isNotEmpty(model.getParentId())){
//      handleVariableAdded(model.getParentId(), name, value, unitOfWork);
//      commandGateway.sendAndWait(new AddVariable(model.getParentId(), name, value));
      brokerSimulator.sendCommand(model.getParentId(), new AddVariable(model.getParentId(), name, value));
    }
//    model.setVersion(model.getVersion() + 1);
//    repository.save(model);
  }

  private void handleVariableUpdated(String id, String name, VariableValue value, UnitOfWork<?> unitOfWork) {
    ElasticModelNested model = getOngoingBatchRecord(unitOfWork, id, false);
    NestedSpecificAttribute nestedSpecificAttribute = model.getProcessVariables().stream()
        .filter(s -> s.getName().equals(name))
        .findFirst()
        .orElseGet(NestedSpecificAttribute::new);
    nestedSpecificAttribute.setName(name);
    nestedSpecificAttribute.setValueDate(null);
    nestedSpecificAttribute.setValueDouble(null);
    nestedSpecificAttribute.setValueLong(null);
    nestedSpecificAttribute.setValueDate(null);
    if (value.stringValue() != null){
      nestedSpecificAttribute.setValueString(value.stringValue());
    } else if (value.longValue() != null){
      nestedSpecificAttribute.setValueLong(value.longValue());
    } else if (value.doubleValue() != null){
      nestedSpecificAttribute.setValueDouble(value.doubleValue());
    } else if (value.dateValue() != null){
      nestedSpecificAttribute.setValueDate(value.dateValue());
    } else {
      throw new IllegalArgumentException();
    }
//    if (StringUtils.isNotEmpty(model.getParentId())){
//      handleVariableUpdated(model.getParentId(), name, value, unitOfWork);
//    }
  }

  @EventHandler
  void handle(ParentSet event, UnitOfWork<?> unitOfWork){
    ElasticModelNested model = getOngoingBatchRecord(unitOfWork, event.id(), false);
//    ElasticModelNested model = repository.findById(event.id()).orElseThrow();
    model.setParentId(event.parentId());
//    model.setVersion(model.getVersion() + 1);
//    repository.save(model);
    ongoingTime.set(System.currentTimeMillis());
  }

  private ElasticModelNested getOngoingBatchRecord(UnitOfWork<?> unitOfWork, String aggregateId,
      boolean create){
    Map<String, ElasticModelNested> currentBatch = unitOfWork.getOrComputeResource(
        "current-es-batch-"+Thread.currentThread().getName(), k -> {
          Map<String, ElasticModelNested> map = new HashMap<>();
          unitOfWork.onPrepareCommit(uow -> {
            // check for changes

            map.values().forEach(model -> model.setVersion(model.getVersion() == null ?
                0 : model.getVersion() + 1));
            try {
              repository.saveAll(map.values());
            } catch (Exception e){
              LOGGER.error("Error", e);
            }
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
