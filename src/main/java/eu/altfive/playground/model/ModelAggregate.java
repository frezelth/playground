package eu.altfive.playground.model;

import eu.altfive.playground.command.AddVariable;
import eu.altfive.playground.command.CreateModel;
import eu.altfive.playground.command.SetParent;
import eu.altfive.playground.command.VariableValue;
import eu.altfive.playground.event.ModelCreated;
import eu.altfive.playground.event.ParentSet;
import eu.altfive.playground.event.VariableAdded;
import eu.altfive.playground.event.VariableUpdated;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.eventsourcing.EventSourcingHandler;
import org.axonframework.messaging.MetaData;
import org.axonframework.messaging.annotation.MetaDataValue;
import org.axonframework.modelling.command.AggregateIdentifier;
import org.axonframework.modelling.command.AggregateLifecycle;
import org.axonframework.spring.stereotype.Aggregate;
import org.springframework.util.StringUtils;

@Aggregate
public class ModelAggregate {

  @AggregateIdentifier
  private String id;
  private String name;
  private String parentId;
  private final Map<String, VariableValue> processVariables = new HashMap<>();

  public ModelAggregate() {
  }

  @CommandHandler
  public ModelAggregate(CreateModel command) {
    AggregateLifecycle.apply(
        new ModelCreated(command.id(), command.name())
    );
  }

  @EventSourcingHandler
  void on(ModelCreated event){
    this.id = event.id();
    this.name = event.name();
  }

  @CommandHandler
  public void handle(AddVariable command){
    if (command.name() == null || command.value() == null || command.value().isEmpty()){
      return;
    }
    VariableValue variableValue = processVariables.get(command.name());
    if (variableValue != null){
      if (!variableValue.equals(command.value())){
        AggregateLifecycle.apply(
            new VariableUpdated(this.id, command.name(), command.value())
        );
      }
    } else {
      AggregateLifecycle.apply(
          new VariableAdded(this.id, command.name(), command.value())
      );
    }
  }

  @EventSourcingHandler
  void on(VariableAdded event){
    this.processVariables.put(event.name(), event.value());
  }

  @EventSourcingHandler
  void on(VariableUpdated event){
    this.processVariables.put(event.name(), event.value());
  }

  @CommandHandler
  public void handle(SetParent command){
    if (StringUtils.hasLength(command.parentId()) && !Objects.equals(command.parentId(), this.parentId)){
      AggregateLifecycle.apply(
          new ParentSet(this.id, command.parentId())
      );
    }
  }

  @EventSourcingHandler
  void on(ParentSet event){
    this.parentId = event.parentId();
  }
}
