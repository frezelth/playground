package eu.altfive.playground.command;

import org.axonframework.modelling.command.TargetAggregateIdentifier;

public record SetParent(
    @TargetAggregateIdentifier
    String id,
    String parentId
) {

}
