package eu.altfive.playground.projection;

import eu.altfive.playground.BrokerSimulator;
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
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.messaging.unitofwork.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.document.Document;
import org.springframework.data.elasticsearch.core.mapping.IndexCoordinates;
import org.springframework.data.elasticsearch.core.query.ScriptType;
import org.springframework.data.elasticsearch.core.query.UpdateQuery;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

//@ProcessingGroup("elastic-nested-nobatch")
//@Component
public class ElasticNoBatchNestedEventHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticNoBatchNestedEventHandler.class);
  private final ElasticModelNestedRepository repository;
  private final BrokerSimulator brokerSimulator;

  private static final AtomicLong startTime = new AtomicLong(0);
  private static final AtomicLong ongoingTime = new AtomicLong(0);
  private static final AtomicLong lastCheckTime = new AtomicLong(0);
  private final ElasticsearchTemplate elasticsearchTemplate;
  private final CommandGateway commandGateway;

  public ElasticNoBatchNestedEventHandler(@Autowired(required=false) ElasticModelNestedRepository repository,
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
  void handle(ModelCreated event){
    if (startTime.longValue() == 0){
      startTime.set(System.currentTimeMillis());
    }
//    ElasticModelNested model = repository.findById(event.getId()).orElse(new ElasticModelNested());
//    model.setId(event.getId());
//    model.setName(event.getName());
//    model.setVersion(model.getVersion() + 1);
//    repository.save(model);

    final String updateScript = "ctx._source.id = params.id; ctx._source.name = params.name;";

    UpdateQuery query = UpdateQuery.builder(event.getId())
        .withScriptedUpsert(Boolean.TRUE)
        .withScript(updateScript)
        .withParams(
            Map.of(
                "id", event.getId(),
                "name", event.getName()
            ))
        .withScriptType(ScriptType.INLINE)
        .withUpsert(Document.create())
        .build();
    elasticsearchTemplate.update(query, IndexCoordinates.of("model-nested"));
    ongoingTime.set(System.currentTimeMillis());
  }

  @EventHandler
  void handle(VariableAdded event){
//    ElasticModelNested model = repository.findById(event.id()).orElseThrow();
//    handleVariableAdded(event.getId(), event.getName(), event.getValue());
//    repository.save(model);

    final String updateScript = "if (ctx._source.processVariables == null){ ctx._source.processVariables = new ArrayList(); } ctx._source.processVariables.add(params.processVariable);";

    NestedSpecificAttribute nestedSpecificAttribute = new NestedSpecificAttribute();
    nestedSpecificAttribute.setName(event.getName());
    nestedSpecificAttribute.setValueDate(null);
    nestedSpecificAttribute.setValueDouble(null);
    nestedSpecificAttribute.setValueLong(null);
    nestedSpecificAttribute.setValueDate(null);

    if (event.getValue().getKindCase() == KindCase.STRINGVALUE){
      nestedSpecificAttribute.setValueString(event.getValue().getStringValue());
    } else if (event.getValue().getKindCase() == KindCase.LONGVALUE){
      nestedSpecificAttribute.setValueLong(event.getValue().getLongValue());
    } else if (event.getValue().getKindCase() == KindCase.DOUBLEVALUE){
      nestedSpecificAttribute.setValueDouble(event.getValue().getDoubleValue());
    } else if (event.getValue().getKindCase() == KindCase.TIMEVALUE){
      nestedSpecificAttribute.setValueDate(new Date(event.getValue().getTimeValue().getSeconds() * 1000));
    } else {
      throw new IllegalArgumentException();
    }

    UpdateQuery query = UpdateQuery.builder(event.getId())
        .withScript(updateScript)
        .withParams(
            Map.of(
                "processVariable", nestedSpecificAttribute,
                "id", event.getId()
            ))
        .withScriptType(ScriptType.INLINE)
        .withScriptedUpsert(Boolean.TRUE)
        .withUpsert(Document.create())
        .build();

    elasticsearchTemplate.update(query, IndexCoordinates.of("model-nested"));

    for (String parentId : event.getParentIdsList()){
      UpdateQuery queryParent = UpdateQuery.builder(parentId)
          .withScript(updateScript)
          .withParams(
              Map.of(
                  "processVariable", nestedSpecificAttribute,
                  "id", parentId
              ))
          .withScriptType(ScriptType.INLINE)
          .withScriptedUpsert(Boolean.TRUE)
          .withUpsert(Document.create())
          .build();

      elasticsearchTemplate.update(query, IndexCoordinates.of("model-nested"));
    }


    ongoingTime.set(System.currentTimeMillis());
  }

  @EventHandler
  void handle(VariableUpdated event, UnitOfWork<?> unitOfWork){
//    ElasticModelNested model = repository.findById(event.id()).orElseThrow();
//    handleVariableUpdated(event.id(), event.name(), event.value(), unitOfWork);
//    repository.save(model);
    ongoingTime.set(System.currentTimeMillis());
  }


  @EventHandler
  void handle(ParentSet event){
//    ElasticModelNested model = repository.findById(event.getId()).orElseThrow();
//    model.setParentId(event.getParentId());
//    model.setVersion(model.getVersion() + 1);
//    repository.save(model);

    final String updateScript = "ctx._source.id = params.id; ctx._source.parentId = params.parentId;";

    UpdateQuery query = UpdateQuery.builder(event.getId())
        .withScriptedUpsert(Boolean.TRUE)
        .withScript(updateScript)
        .withParams(
            Map.of(
                "id", event.getId(),
                "parentId", event.getParentId()
            ))
        .withScriptType(ScriptType.INLINE)
        .withUpsert(Document.create())
        .build();

    elasticsearchTemplate.update(query, IndexCoordinates.of("model-nested"));

    ongoingTime.set(System.currentTimeMillis());
  }

}
