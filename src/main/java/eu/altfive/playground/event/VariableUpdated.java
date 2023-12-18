package eu.altfive.playground.event;

import eu.altfive.playground.command.VariableValue;

public record VariableUpdated(
    String id,
    String name,
    VariableValue value
) {

}
