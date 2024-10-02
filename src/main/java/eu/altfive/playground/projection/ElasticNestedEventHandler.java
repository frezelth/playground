package eu.altfive.playground.projection;

import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.eventhandling.SequenceNumber;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.messaging.annotation.SourceId;
import org.axonframework.messaging.unitofwork.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.elasticsearch.BulkFailureException;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@ProcessingGroup("elastic-nested")
@Component
public class ElasticNestedEventHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticNestedEventHandler.class);
  private final ElasticModelNestedRepository repository;
  private final ObjectMapper objectMapper = new ObjectMapper();

  private static final AtomicLong startTime = new AtomicLong(0);
  private static final AtomicLong ongoingTime = new AtomicLong(0);
  private static final AtomicLong lastCheckTime = new AtomicLong(0);
  private final ElasticsearchTemplate elasticsearchTemplate;
  private final CommandGateway commandGateway;

  public ElasticNestedEventHandler(
      ElasticsearchTemplate elasticsearchTemplate,
      CommandGateway commandGateway,
      ElasticModelNestedRepository repository
  ) {
    this.repository = repository;
    this.elasticsearchTemplate = elasticsearchTemplate;
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
  void handle(ModelCreated event, @SourceId String aggregateIdentifier, @SequenceNumber Long sequenceNumber, UnitOfWork<?> unitOfWork){
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

    ElasticModelNested model = getOngoingBatchRecord(unitOfWork, event.getId(), sequenceNumber, true);

    if (model.getLastEventsPerAggregateStored().get(aggregateIdentifier) != null &&
        model.getLastEventsPerAggregateStored().get(aggregateIdentifier) >= sequenceNumber){
      return;
    }

    model.setId(event.getId());
    model.setName(event.getName());
    model.setLastEventsPerAggregate(new HashMap<>());
    model.getLastEventsPerAggregate().put(aggregateIdentifier, sequenceNumber);
    model.setModified(true);

    ongoingTime.set(System.currentTimeMillis());
  }

  @EventHandler
  void handle(VariableAdded event, @SourceId String aggregateIdentifier, @SequenceNumber Long sequenceNumber, UnitOfWork<?> unitOfWork){
//    ElasticModelNested model = repository.findById(event.id()).orElseThrow();
//    ElasticModelNested model = getOngoingBatchRecord(unitOfWork, event.getId(), false);
    handleVariableAdded(event.getId(), event.getName(), event.getValue(), unitOfWork, aggregateIdentifier, sequenceNumber);
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

  private void handleVariableAdded(String id, String name, VariableValue value, UnitOfWork<?> unitOfWork,
      String aggregateIdentifier, Long sequenceNumber) {
    ElasticModelNested model = getOngoingBatchRecord(unitOfWork, id, sequenceNumber, false);

    //      commandGateway.sendAndWait(new AddVariable(model.getParentId(), name, value));
    //      brokerSimulator.sendCommand(model.getParentId(), new AddVariable(model.getParentId(), name, value));
    if (model.getLastEventsPerAggregateStored().get(aggregateIdentifier) == null ||
        model.getLastEventsPerAggregateStored().get(aggregateIdentifier) < sequenceNumber) {

//    ElasticModelNested model = repository.findById(modelId).orElseThrow();
      if (model.getProcessVariables() == null) {
        model.setProcessVariables(new ArrayList<>());
      }
      NestedSpecificAttribute nestedSpecificAttribute = new NestedSpecificAttribute();
      nestedSpecificAttribute.setName(name);
      nestedSpecificAttribute.setValueDate(null);
      nestedSpecificAttribute.setValueDouble(null);
      nestedSpecificAttribute.setValueLong(null);
      nestedSpecificAttribute.setValueDate(null);

      if (value.getKindCase() == KindCase.STRINGVALUE) {
        nestedSpecificAttribute.setValueString(value.getStringValue());
      } else if (value.getKindCase() == KindCase.LONGVALUE) {
        nestedSpecificAttribute.setValueLong(value.getLongValue());
      } else if (value.getKindCase() == KindCase.DOUBLEVALUE) {
        nestedSpecificAttribute.setValueDouble(value.getDoubleValue());
      } else if (value.getKindCase() == KindCase.TIMEVALUE) {
        nestedSpecificAttribute.setValueDate(new Date(value.getTimeValue().getSeconds() * 1000));
      } else {
        throw new IllegalArgumentException();
      }

      model.getLastEventsPerAggregate().put(aggregateIdentifier, sequenceNumber);
      model.setModified(true);

      model.getProcessVariables().add(nestedSpecificAttribute);
      //    model.setVersion(model.getVersion() + 1);
//    repository.save(model);
    }
    if (StringUtils.isNotEmpty(model.getParentId())){
      handleVariableAdded(model.getParentId(), name, value, unitOfWork, aggregateIdentifier, sequenceNumber);
    }

  }

  @EventHandler
  void handle(VariableUpdated event, UnitOfWork<?> unitOfWork){
//    ElasticModelNested model = repository.findById(event.id()).orElseThrow();
//    handleVariableUpdated(event.id(), event.name(), event.value(), unitOfWork);
//    repository.save(model);
    ongoingTime.set(System.currentTimeMillis());
  }

  @EventHandler
  void handle(ParentSet event, @SourceId String aggregateIdentifier, @SequenceNumber Long sequenceNumber, UnitOfWork<?> unitOfWork){
    ElasticModelNested model = getOngoingBatchRecord(unitOfWork, event.getId(), sequenceNumber, false);

    if (model.getLastEventsPerAggregateStored().get(aggregateIdentifier) != null &&
        model.getLastEventsPerAggregateStored().get(aggregateIdentifier) >= sequenceNumber){
      return;
    }

    model.getLastEventsPerAggregate().put(aggregateIdentifier, sequenceNumber);
//    ElasticModelNested model = repository.findById(event.id()).orElseThrow();
    model.setParentId(event.getParentId());
    model.setModified(true);
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
      Long sequenceNumber,
      boolean create){

    Map<String, BatchRecord> currentBatch = unitOfWork.getOrComputeResource(
        "current-es-batch-"+Thread.currentThread().getName(), k -> {
          Map<String, BatchRecord> map = new HashMap<>();
          unitOfWork.onPrepareCommit(uow -> {
            // check for changes

            Set<ElasticModelNested> toSave = map.values()
                .stream()
                // only process events that have not been processed yet
                .filter(record ->
                  // for each agg
                  record.record().isModified()
                )
                .peek(model -> model.record().setVersion(model.record().getVersion() == null ?
                    0 : model.record().getVersion() + 1))
                .map(BatchRecord::record)
                .collect(Collectors.toSet());

            if (toSave.size() < map.size()){
              LOGGER.info("Saving less");
            }

            try {
              repository.saveAll(toSave);
            } catch (BulkFailureException e){
//              Set<String> processedDocuments = toSave.stream()
//                  .map(ElasticModelNested::getId)
//                  .collect(Collectors.toSet());
//              processedDocuments.removeAll(e.getFailedDocuments().keySet());
              LOGGER.error("Error", e);
              throw e;
            }
          });
          return map;
        });
    if (currentBatch.get(aggregateId) == null){
      ElasticModelNested model = repository.findById(aggregateId).orElseGet(() -> {
        ElasticModelNested elasticModelNested = new ElasticModelNested();
        elasticModelNested.setId(aggregateId);
        elasticModelNested.setLastEventsPerAggregate(new HashMap<>());
        return elasticModelNested;
      });
      model.setLastEventsPerAggregateStored(new HashMap<>(model.getLastEventsPerAggregate()));
      currentBatch.put(aggregateId, new BatchRecord(model, new HashMap<>(model.getLastEventsPerAggregate())));
    }
//    if (create){
//      currentBatch.put(aggregateId, new ElasticModelNested());
//    } else if (currentBatch.get(aggregateId) == null){
//      currentBatch.put(aggregateId, repository.findById(aggregateId).orElseThrow());
//    }
    return currentBatch.get(aggregateId).record();
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

  public record BatchRecord(
      ElasticModelNested record,
      Map<String,Long> currentStoredEventSequencePerAggregate
  ){}

}
