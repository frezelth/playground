package eu.altfive.playground.projection;

import com.fasterxml.jackson.databind.ObjectMapper;
import eu.altfive.playground.BrokerSimulator;
import eu.altfive.playground.command.AddVariable;
import eu.altfive.playground.event.ModelCreated;
import eu.altfive.playground.event.ParentSet;
import eu.altfive.playground.event.VariableAdded;
import eu.altfive.playground.event.VariableUpdated;
import eu.altfive.playground.projection.model.ElasticModelNested;
import eu.altfive.playground.projection.model.ElasticModelNested.NestedSpecificAttribute;
import eu.altfive.playground.projection.repository.ElasticModelNestedRepository;
import eu.europa.ec.cc.variables.proto.VariableValue;
import eu.europa.ec.cc.variables.proto.VariableValue.KindCase;
import io.micrometer.common.util.StringUtils;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.messaging.unitofwork.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.ScriptType;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@ProcessingGroup("elastic-nested")
@Component
public class ElasticNestedEventHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticNestedEventHandler.class);
  private final ElasticModelNestedRepository repository;
  private final BrokerSimulator brokerSimulator;
  private final ObjectMapper objectMapper = new ObjectMapper();

  private static final AtomicLong startTime = new AtomicLong(0);
  private static final AtomicLong ongoingTime = new AtomicLong(0);
  private static final AtomicLong lastCheckTime = new AtomicLong(0);
  private final ElasticsearchTemplate elasticsearchTemplate;
  private final CommandGateway commandGateway;

  public ElasticNestedEventHandler(
      ElasticsearchTemplate elasticsearchTemplate,
      BrokerSimulator brokerSimulator,
      CommandGateway commandGateway,
      ElasticModelNestedRepository repository
  ) {
    this.repository = repository;
    this.elasticsearchTemplate = elasticsearchTemplate;
    this.brokerSimulator = brokerSimulator;
    this.commandGateway = commandGateway;
  }

  @Scheduled(initialDelay = 10000, fixedDelay = 5000)
  public void checkChanges(){
    if (lastCheckTime.longValue() == ongoingTime.longValue()){
      LOGGER.info("finished ES nested model indexing:"+(ongoingTime.longValue() - startTime.longValue()));
    }
    lastCheckTime.set(ongoingTime.longValue());
  }

  @EventHandler
  void handle(ModelCreated event, UnitOfWork<?> unitOfWork){
    if (startTime.longValue() == 0){
      startTime.set(System.currentTimeMillis());
    }

//    final String updateScript = "ctx._source.id = params.id; ctx._source.name = params.name;";
//
//    UpdateQuery query = UpdateQuery.builder(event.getId())
//        .withScriptedUpsert(Boolean.TRUE)
//        .withScript(updateScript)
//        .withParams(
//            Map.of(
//                "id", event.getId(),
//                "name", event.getName()
//            ))
//        .withScriptType(ScriptType.INLINE)
//        .withUpsert(Document.create())
//        .build();

    ElasticModelNested model = getOngoingBatchRecord(unitOfWork, event.getId(), true);
    model.setId(event.getId());
    model.setName(event.getName());

    ongoingTime.set(System.currentTimeMillis());
  }

  @EventHandler
  void handle(VariableAdded event, UnitOfWork<?> unitOfWork){
//    ElasticModelNested model = repository.findById(event.id()).orElseThrow();
//    ElasticModelNested model = getOngoingBatchRecord(unitOfWork, event.getId(), false);
    handleVariableAdded(event.getId(), event.getName(), event.getValue(), unitOfWork);
//    repository.save(model);

//    final String updateScript = "if (ctx._source.processVariables == null){ ctx._source.processVariables = new ArrayList(); } ctx._source.processVariables.add(params.processVariable);";
//
//    NestedSpecificAttribute nestedSpecificAttribute = new NestedSpecificAttribute();
//    nestedSpecificAttribute.setName(event.getName());
//    nestedSpecificAttribute.setValueDate(null);
//    nestedSpecificAttribute.setValueDouble(null);
//    nestedSpecificAttribute.setValueLong(null);
//    nestedSpecificAttribute.setValueDate(null);
//
//    if (event.getValue().getKindCase() == KindCase.STRINGVALUE){
//      nestedSpecificAttribute.setValueString(event.getValue().getStringValue());
//    } else if (event.getValue().getKindCase() == KindCase.LONGVALUE){
//      nestedSpecificAttribute.setValueLong(event.getValue().getLongValue());
//    } else if (event.getValue().getKindCase() == KindCase.DOUBLEVALUE){
//      nestedSpecificAttribute.setValueDouble(event.getValue().getDoubleValue());
//    } else if (event.getValue().getKindCase() == KindCase.TIMEVALUE){
//      nestedSpecificAttribute.setValueDate(new Date(event.getValue().getTimeValue().getSeconds() * 1000));
//    } else {
//      throw new IllegalArgumentException();
//    }
//
//    UpdateQuery query = UpdateQuery.builder(event.getId())
//        .withScript(updateScript)
//        .withParams(
//            Map.of(
//                "processVariable", nestedSpecificAttribute,
//                "id", event.getId()
//            ))
//        .withScriptType(ScriptType.INLINE)
//        .withScriptedUpsert(Boolean.TRUE)
//        .withUpsert(Document.create())
//        .build();
//
//    addOperation(unitOfWork, query);
//
//    for (String parentId : event.getParentIdsList()){
//      UpdateQuery queryParent = UpdateQuery.builder(parentId)
//          .withScript(updateScript)
//          .withParams(
//              Map.of(
//                  "processVariable", nestedSpecificAttribute,
//                  "id", parentId
//              ))
//          .withScriptType(ScriptType.INLINE)
//          .withScriptedUpsert(Boolean.TRUE)
//          .withUpsert(Document.create())
//          .build();
//
//      addOperation(unitOfWork, queryParent);
//    }

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

    if (value.getKindCase() == KindCase.STRINGVALUE){
      nestedSpecificAttribute.setValueString(value.getStringValue());
    } else if (value.getKindCase() == KindCase.LONGVALUE){
      nestedSpecificAttribute.setValueLong(value.getLongValue());
    } else if (value.getKindCase() == KindCase.DOUBLEVALUE){
      nestedSpecificAttribute.setValueDouble(value.getDoubleValue());
    } else if (value.getKindCase() == KindCase.TIMEVALUE){
      nestedSpecificAttribute.setValueDate(new Date(value.getTimeValue().getSeconds() * 1000));
    } else {
      throw new IllegalArgumentException();
    }

    model.getProcessVariables().add(nestedSpecificAttribute);
    if (StringUtils.isNotEmpty(model.getParentId())){
      handleVariableAdded(model.getParentId(), name, value, unitOfWork);
//      commandGateway.sendAndWait(new AddVariable(model.getParentId(), name, value));
//      brokerSimulator.sendCommand(model.getParentId(), new AddVariable(model.getParentId(), name, value));
    }
//    model.setVersion(model.getVersion() + 1);
//    repository.save(model);
  }

  @EventHandler
  void handle(VariableUpdated event, UnitOfWork<?> unitOfWork){
//    ElasticModelNested model = repository.findById(event.id()).orElseThrow();
//    handleVariableUpdated(event.id(), event.name(), event.value(), unitOfWork);
//    repository.save(model);
    ongoingTime.set(System.currentTimeMillis());
  }

  @EventHandler
  void handle(ParentSet event, UnitOfWork<?> unitOfWork){
    ElasticModelNested model = getOngoingBatchRecord(unitOfWork, event.getId(), false);
//    ElasticModelNested model = repository.findById(event.id()).orElseThrow();
    model.setParentId(event.getParentId());
//    model.setVersion(model.getVersion() + 1);
//    repository.save(model);

//    final String updateScript = "ctx._source.id = params.id; ctx._source.parentId = params.parentId;";
//
//    UpdateQuery query = UpdateQuery.builder(event.getId())
//        .withScriptedUpsert(Boolean.TRUE)
//        .withScript(updateScript)
//        .withParams(
//            Map.of(
//                "id", event.getId(),
//                "parentId", event.getParentId()
//            ))
//        .withScriptType(ScriptType.INLINE)
//        .withUpsert(Document.create())
//        .build();
//
//    addOperation(unitOfWork, query);

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
    currentBatch.put(aggregateId, repository.findById(aggregateId)
        .orElseGet(() -> {
          ElasticModelNested elasticModelNested = new ElasticModelNested();
          elasticModelNested.setId(aggregateId);
          return elasticModelNested;
        }));
//    if (create){
//      currentBatch.put(aggregateId, new ElasticModelNested());
//    } else if (currentBatch.get(aggregateId) == null){
//      currentBatch.put(aggregateId, repository.findById(aggregateId).orElseThrow());
//    }
    return currentBatch.get(aggregateId);
  }


  private void addOperation(UnitOfWork<?> unitOfWork, UpdateQuery updateQuery){
    List<UpdateQuery> currentBatch = unitOfWork.getOrComputeResource(
        "current-es-batch-"+Thread.currentThread().getName(), k -> {
          List<UpdateQuery> newBatch = new ArrayList<>();
          unitOfWork.onPrepareCommit(uow ->
              elasticsearchTemplate.bulkUpdate(newBatch, IndexCoordinates.of("model-nested"))
          );
          return newBatch;
        });

    currentBatch.add(updateQuery);
  }

}
