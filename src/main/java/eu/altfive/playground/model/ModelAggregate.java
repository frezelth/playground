package eu.altfive.playground.model;

import eu.altfive.playground.command.AddVariable;
import eu.altfive.playground.command.CreateModel;
import eu.altfive.playground.command.SetParent;
import eu.altfive.playground.event.ModelCreated;
import eu.altfive.playground.event.ParentSet;
import eu.altfive.playground.event.VariableAdded;
import java.util.Objects;
import org.axonframework.commandhandling.CommandHandler;
import org.axonframework.eventsourcing.EventSourcingHandler;
import org.axonframework.modelling.command.AggregateIdentifier;
import org.axonframework.modelling.command.AggregateLifecycle;
import org.axonframework.spring.stereotype.Aggregate;
import org.springframework.util.StringUtils;

@Aggregate(cache = "axonCache")
public class ModelAggregate {

  @AggregateIdentifier
  private String id;
  private String name;
  private String parentId;

//  private final Map<String, VariableValue> processVariables = new HashMap<>();

  public ModelAggregate() {
  }

  @CommandHandler
  public ModelAggregate(CreateModel command) {
    AggregateLifecycle.apply(
        new ModelCreated(command.id(), command.name())
    );
  }

  @EventSourcingHandler
  public void on(ModelCreated event){
    this.id = event.getId();
    this.name = event.getName();
  }

  @CommandHandler
  public void handle(AddVariable command){
    if (command.name() == null || command.value() == null){
      return;
    }
//    VariableValue variableValue = processVariables.get(command.name());

//    eu.europa.ec.cc.variables.proto.VariableValue val;
//    if (command.value().getStringValue() != null && !command.value().getStringValue().isEmpty()){
//      val = eu.europa.ec.cc.variables.proto.VariableValue.newBuilder()
//          .setStringValue(command.value().getStringValue())
//          .build();
//    } else if (command.value().getLongValue() != null){
//      val = eu.europa.ec.cc.variables.proto.VariableValue.newBuilder()
//          .setLongValue(command.value().getLongValue())
//          .build();
//    } else if (command.value().getDoubleValue() != null){
//      val = eu.europa.ec.cc.variables.proto.VariableValue.newBuilder()
//          .setDoubleValue(command.value().getDoubleValue())
//          .build();
//    } else {
//      val = eu.europa.ec.cc.variables.proto.VariableValue.newBuilder()
//          .setTimeValue(Timestamp.newBuilder()
//              .setSeconds(command.value().getDateValue().getTime() / 1000)
//              .build())
//          .build();
//    }

//    if (variableValue != null){
//      if (!variableValue.equals(command.value())){
//        AggregateLifecycle.apply(
//            VariableUpdated.newBuilder()
//                .setId(this.id)
//                .setName(command.name())
//                .setValue(command.value())
//                .build()
//        );
//      }
//    } else {
      AggregateLifecycle.apply(
          new VariableAdded(this.id, command.name(), command.value())
      );
//    }
  }

//  @EventSourcingHandler
//  void on(VariableAdded event){
//    this.processVariables.put(event.getName(), event.getValue());
//  }
//
//  @EventSourcingHandler
//  void on(VariableUpdated event){
//    this.processVariables.put(event.getName(), event.getValue());
//  }

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
    this.parentId = event.getParentId();
  }

  public String getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public String getParentId() {
    return parentId;
  }

//  public Map<String, VariableValue> getProcessVariables() {
//    return processVariables;
//  }
}
