package eu.altfive.playground.event;

import eu.altfive.playground.command.VariableValue;

public class VariableAdded {

  private String id;
  private String name;
  private VariableValue value;

  public VariableAdded() {
  }

  public VariableAdded(String id, String name, VariableValue value) {
    this.id = id;
    this.name = name;
    this.value = value;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public VariableValue getValue() {
    return value;
  }

  public void setValue(VariableValue value) {
    this.value = value;
  }
}
