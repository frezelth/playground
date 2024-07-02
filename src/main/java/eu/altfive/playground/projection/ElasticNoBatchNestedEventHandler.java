package eu.altfive.playground.projection;

import eu.altfive.playground.BrokerSimulator;
import eu.altfive.playground.command.AddVariable;
import eu.altfive.playground.command.VariableValue;
import eu.altfive.playground.event.ModelCreated;
import eu.altfive.playground.event.ParentSet;
import eu.altfive.playground.event.VariableAdded;
import eu.altfive.playground.event.VariableUpdated;
import eu.altfive.playground.projection.model.ElasticModelNested;
import eu.altfive.playground.projection.model.ElasticModelNested.NestedSpecificAttribute;
import eu.altfive.playground.projection.repository.ElasticModelNestedRepository;
import io.micrometer.common.util.StringUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.axonframework.commandhandling.gateway.CommandGateway;
import org.axonframework.config.ProcessingGroup;
import org.axonframework.eventhandling.EventHandler;
import org.axonframework.messaging.unitofwork.UnitOfWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.elasticsearch.client.elc.ElasticsearchTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@ProcessingGroup("elastic-nested-nobatch")
@Component
public class ElasticNoBatchNestedEventHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(ElasticNoBatchNestedEventHandler.class);
  private final ElasticModelNestedRepository repository;
  private final BrokerSimulator brokerSimulator;

  private static final AtomicLong startTime = new AtomicLong(0);
  private static final AtomicLong ongoingTime = new AtomicLong(0);
  private static final AtomicLong lastCheckTime = new AtomicLong(0);
  private final ElasticsearchTemplate elasticsearchTemplate;
  private final CommandGateway commandGateway;

  public ElasticNoBatchNestedEventHandler(ElasticModelNestedRepository repository,
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
    ElasticModelNested model = repository.findById(event.id()).orElse(new ElasticModelNested());
    model.setId(event.id());
    model.setName(event.name());
    model.setVersion(model.getVersion() + 1);
    repository.save(model);
    ongoingTime.set(System.currentTimeMillis());
  }

  @EventHandler
  void handle(VariableAdded event){
//    ElasticModelNested model = repository.findById(event.id()).orElseThrow();
    handleVariableAdded(event.id(), event.name(), event.value());
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

  private void handleVariableAdded(String id, String name, VariableValue value) {
    ElasticModelNested model = repository.findById(id).orElseGet(() -> {
      ElasticModelNested agg = new ElasticModelNested();
      agg.setId(id);
      return agg;
    });
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
      handleVariableAdded(model.getParentId(), name, value);
    }
    model.setVersion(model.getVersion() + 1);
    repository.save(model);
  }

  @EventHandler
  void handle(ParentSet event){
    ElasticModelNested model = repository.findById(event.id()).orElseThrow();
    model.setParentId(event.parentId());
    model.setVersion(model.getVersion() + 1);
    repository.save(model);
    ongoingTime.set(System.currentTimeMillis());
  }

}
