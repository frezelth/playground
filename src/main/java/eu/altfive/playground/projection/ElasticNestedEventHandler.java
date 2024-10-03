package eu.altfive.playground.projection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Message;
import eu.altfive.playground.command.VariableValue;
import eu.altfive.playground.event.ModelCreated;
import eu.altfive.playground.event.ParentSet;
import eu.altfive.playground.event.VariableAdded;
import eu.altfive.playground.projection.model.ElasticModelNested;
import eu.altfive.playground.projection.model.ElasticModelNested.NestedSpecificAttribute;
import eu.altfive.playground.projection.repository.ElasticModelNestedRepository;
import io.micrometer.common.util.StringUtils;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.DomainEventMessage;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.eventhandling.GenericDomainEventMessage;
import org.axonframework.eventhandling.GenericEventMessage;
import org.axonframework.eventhandling.SequenceNumber;
import org.axonframework.eventhandling.TrackingToken;
import org.axonframework.extensions.kafka.eventhandling.KafkaMessageConverter;
import org.axonframework.extensions.kafka.eventhandling.producer.KafkaPublisher;
import org.axonframework.messaging.MetaData;
import org.axonframework.messaging.annotation.SourceId;
import org.axonframework.messaging.unitofwork.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.event.EventListener;
import org.springframework.data.elasticsearch.BulkFailureException;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

//@ProcessingGroup("elastic-nested")
@Component
public class ElasticNestedEventHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticNestedEventHandler.class);
  private final ElasticModelNestedRepository repository;
  private final ObjectMapper objectMapper = new ObjectMapper();

  private static final AtomicLong startTime = new AtomicLong(0);
  private static final AtomicLong ongoingTime = new AtomicLong(0);
  private static final AtomicLong lastCheckTime = new AtomicLong(0);
  private final ElasticsearchTemplate elasticsearchTemplate;
  private final KafkaTemplate<String, byte[]> kafkaTemplate;
  private final KafkaMessageConverter<String, byte[]> messageConverter;

  public ElasticNestedEventHandler(
      ElasticsearchTemplate elasticsearchTemplate,
      KafkaTemplate<String, byte[]> kafkaTemplate,
      ElasticModelNestedRepository repository,
      KafkaMessageConverter<String, byte[]> messageConverter
  ) {
    this.repository = repository;
    this.elasticsearchTemplate = elasticsearchTemplate;
    this.kafkaTemplate = kafkaTemplate;
    this.messageConverter = messageConverter;
  }

  @Scheduled(initialDelay = 10000, fixedDelay = 5000)
  public void checkChanges(){
    if (lastCheckTime.longValue() == ongoingTime.longValue()){
      LOGGER.info("finished ES nested model indexing:"+(ongoingTime.longValue() - startTime.longValue()));
    }
    lastCheckTime.set(ongoingTime.longValue());
  }

  @EventListener
  void handle(
      ModelCreated event){
    if (startTime.longValue() == 0){
      startTime.set(System.currentTimeMillis());
    }
//    ElasticModelNested model = documents.get(event.getId());
    ElasticModelNested model = ElasticProjection.currentDocument.get();
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

    model.setId(event.getId());
    model.setName(event.getName());

    ongoingTime.set(System.currentTimeMillis());
  }

  @EventListener
  void handle(
      VariableAdded event){
//    ElasticModelNested model = repository.findById(event.id()).orElseThrow();
//    ElasticModelNested model = getOngoingBatchRecord(unitOfWork, event.getId(), false);
    handleVariableAdded(event.getName(), event.getValue());
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

  private void handleVariableAdded(
      String name, VariableValue value) {
    ElasticModelNested model = ElasticProjection.currentDocument.get();

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
    if (value.getStringValue() != null){
      nestedSpecificAttribute.setValueString(value.getStringValue());
    } else if (value.getLongValue() != null){
      nestedSpecificAttribute.setValueLong(value.getLongValue());
    } else if (value.getDoubleValue() != null){
      nestedSpecificAttribute.setValueDouble(value.getDoubleValue());
    } else if (value.getDateValue() != null){
      nestedSpecificAttribute.setValueDate(value.getDateValue());
    } else {
      throw new IllegalArgumentException();
    }

    model.getProcessVariables().add(nestedSpecificAttribute);
    if (StringUtils.isNotEmpty(model.getParentId())){
      GenericDomainEventMessage<Object> genericDomainEventMessage = new GenericDomainEventMessage<>(
          VariableAdded.class.getTypeName(),
          model.getParentId(),
          0,
          new VariableAdded(model.getParentId(), name, value),
          MetaData.emptyInstance()
      );
      ProducerRecord<String, byte[]> producerRecord = messageConverter.createKafkaMessage(
          genericDomainEventMessage, "cc-local-event");
      kafkaTemplate.send(producerRecord);
//      kafkaTemplate.send(
//          new ProducerRecord<>(
//              "cc-local-event",
//              null, model.getParentId(),
//              VariableAdded.newBuilder()
//                  .setId(model.getParentId())
//                  .setName(name)
//                  .setValue(value)
//                  .build().toByteArray(),
//              List.of(
//                  new RecordHeader("axon-message-id", "notused".getBytes(StandardCharsets.ISO_8859_1)),
//                  new RecordHeader("axon-message-aggregate-id", model.getParentId().getBytes(StandardCharsets.ISO_8859_1)),
//                  new RecordHeader("axon-message-type",VariableAdded.class.getTypeName().getBytes(
//                      StandardCharsets.ISO_8859_1))
//              ))
//          );
    }

  }

  @EventListener
  void handle(ParentSet event){
    ElasticModelNested model = ElasticProjection.currentDocument.get();

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
