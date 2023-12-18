package eu.altfive.playground.command;

import java.util.Date;

public record VariableValue(
    String stringValue,
    Long longValue,
    Double doubleValue,
    Date dateValue
) {

  public boolean isEmpty(){
    return stringValue == null && longValue == null && doubleValue == null
        && dateValue == null;
  }
}
