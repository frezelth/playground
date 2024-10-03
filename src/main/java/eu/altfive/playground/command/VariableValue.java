package eu.altfive.playground.command;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.Date;

public class VariableValue {

  private String stringValue;
  private Long longValue;
  private Double doubleValue;
  private Date dateValue;

  public VariableValue() {
  }

  public VariableValue(String stringValue, Long longValue, Double doubleValue, Date dateValue) {
    this.stringValue = stringValue;
    this.longValue = longValue;
    this.doubleValue = doubleValue;
    this.dateValue = dateValue;
  }

  public String getStringValue() {
    return stringValue;
  }

  public void setStringValue(String stringValue) {
    this.stringValue = stringValue;
  }

  public Long getLongValue() {
    return longValue;
  }

  public void setLongValue(Long longValue) {
    this.longValue = longValue;
  }

  public Double getDoubleValue() {
    return doubleValue;
  }

  public void setDoubleValue(Double doubleValue) {
    this.doubleValue = doubleValue;
  }

  public Date getDateValue() {
    return dateValue;
  }

  public void setDateValue(Date dateValue) {
    this.dateValue = dateValue;
  }

  @JsonIgnore
  public boolean isEmpty(){
    return stringValue == null && longValue == null && doubleValue == null
        && dateValue == null;
  }
}
