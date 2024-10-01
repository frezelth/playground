package eu.altfive.playground.command;

import eu.europa.ec.cc.variables.proto.VariableValue;
import org.axonframework.modelling.command.TargetAggregateIdentifier;

public record AddVariable(
    @TargetAggregateIdentifier
    String id,
    String name,
    VariableValue value
) {

}
