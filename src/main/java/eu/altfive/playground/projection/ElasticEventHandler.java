package eu.altfive.playground.projection;

import eu.altfive.playground.command.AddVariable;
import eu.altfive.playground.command.VariableValue;
import eu.altfive.playground.event.ModelCreated;
import eu.altfive.playground.event.ParentSet;
import eu.altfive.playground.event.VariableAdded;
import eu.altfive.playground.event.VariableUpdated;
import eu.altfive.playground.projection.model.ElasticModel;
import eu.altfive.playground.projection.model.ElasticModelNested;
import eu.altfive.playground.projection.repository.ElasticModelRepository;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.messaging.unitofwork.UnitOfWork;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.RefreshPolicy;
import org.springframework.data.elasticsearch.core.query.BulkOptions;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.IndexQueryBuilder;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

//@ProcessingGroup("elastic-model")
//@Component
public class ElasticEventHandler {

  private static final AtomicLong startTime = new AtomicLong(0);
  private static final AtomicLong ongoingTime = new AtomicLong(0);
  private static final AtomicLong lastCheckTime = new AtomicLong(0);

  private final CommandGateway commandGateway;
  private final ElasticModelRepository repository;

  public ElasticEventHandler(CommandGateway commandGateway, ElasticModelRepository repository) {
    this.commandGateway = commandGateway;
    this.repository = repository;
  }

  @Scheduled(initialDelay = 10000, fixedDelay = 5000)
  public void checkChanges(){
    if (lastCheckTime.longValue() == ongoingTime.longValue()){
      System.out.println("finished ES model indexing:"+(ongoingTime.longValue() - startTime.longValue()));
    }
    lastCheckTime.set(ongoingTime.longValue());
  }

  @EventHandler
  void handle(ModelCreated event, UnitOfWork<?> unitOfWork){
    if (startTime.longValue() == 0){
      startTime.set(System.currentTimeMillis());
    }
    ElasticModel model = getOngoingBatchRecord(unitOfWork, event.id(), true);
//    ElasticModel model = new ElasticModel();
    model.setId(event.id());
    model.setName(event.name());
//    repository.save(model);
    ongoingTime.set(System.currentTimeMillis());
  }

  @EventHandler
  void handle(VariableAdded event, UnitOfWork<?> unitOfWork){
//    ElasticModel model = repository.findById(event.id()).orElseThrow();
    ElasticModel model = getOngoingBatchRecord(unitOfWork, event.id(), false);
    if (model.getProcessVariables() == null){
      model.setProcessVariables(new HashMap<>());
    }
    Serializable valueFromVariableValue = getValueFromVariableValue(event.value());
    model.getProcessVariables().put(event.name(), valueFromVariableValue);
    if (StringUtils.hasLength(model.getParentId())){
//      commandGateway.send(new AddVariable(model.getParentId(),
//          event.name(), event.value()));
      handle(new VariableAdded(model.getParentId(), event.name(), event.value()), unitOfWork);
    }
//    repository.save(model);
    ongoingTime.set(System.currentTimeMillis());
  }

  @EventHandler
  void handle(ParentSet event, UnitOfWork<?> unitOfWork){
    ElasticModel model = getOngoingBatchRecord(unitOfWork, event.id(), false);
//    ElasticModelNested model = repository.findById(event.id()).orElseThrow();
    model.setParentId(event.parentId());
//    model.setVersion(model.getVersion() + 1);
//    repository.save(model);
    ongoingTime.set(System.currentTimeMillis());
  }

  @EventHandler
  void handle(VariableUpdated event, UnitOfWork<?> unitOfWork){
    ElasticModel model = repository.findById(event.id()).orElseThrow();
//    ElasticModel model = getOngoingBatchRecord(unitOfWork, event.id(), false);
    if (model.getProcessVariables() == null){
      model.setProcessVariables(new HashMap<>());
    }
    model.getProcessVariables().put(event.name(), getValueFromVariableValue(event.value()));
    repository.save(model);
    ongoingTime.set(System.currentTimeMillis());
  }

  private ElasticModel getOngoingBatchRecord(UnitOfWork<?> unitOfWork, String aggregateId,
      boolean create){
    Map<String, ElasticModel> currentBatch = unitOfWork.getOrComputeResource(
        "current-es-batch", k -> {
          Map<String, ElasticModel> map = new HashMap<>();
          unitOfWork.onPrepareCommit(uow -> {
            map.values().forEach(model -> model.setVersion(model.getVersion() == null ?
                0 : model.getVersion() + 1));
            repository.saveAll(map.values());
          });
          return map;
        });
    if (create){
      currentBatch.put(aggregateId, new ElasticModel());
    } else if (currentBatch.get(aggregateId) == null){
      currentBatch.put(aggregateId, repository.findById(aggregateId).orElseThrow());
    }
    return currentBatch.get(aggregateId);
  }

  private Serializable getValueFromVariableValue(VariableValue variableValue){
    if (variableValue.stringValue() != null){
      return variableValue.stringValue();
    } else if (variableValue.doubleValue() != null){
      return variableValue.doubleValue();
    } else if (variableValue.longValue() != null){
      return variableValue.longValue();
    } else {
      return variableValue.dateValue();
    }
  }

}
