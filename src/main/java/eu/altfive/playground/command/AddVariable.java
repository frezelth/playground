package eu.altfive.playground.command;

import org.axonframework.modelling.command.TargetAggregateIdentifier;

public record AddVariable(
    @TargetAggregateIdentifier
    String id,
    String name,
    VariableValue value
) {

}
