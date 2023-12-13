package eu.altfive.playground.model;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

public class SpecificAttributes {

  private static final String DATE_PREFIX = "DATE___";
  private static final String STRING_PREFIX = "STRING___";
  private static final String LONG_PREFIX = "LONG___";
  private static final String DOUBLE_PREFIX = "DOUBLE___";
  private static final String INTEGER_PREFIX = "INTEGER___";
  private static final String BOOLEAN_PREFIX = "BOOLEAN___";
  @Field(type = FieldType.Flattened)
  private final Map<String, Serializable> attributes;

  public SpecificAttributes(Map<String, Serializable> attributes) {
    this.attributes = attributes;
  }

  public Map<String, Serializable> getAttributes(){
    return attributes;
  }

  public Serializable addDate(String name, Date value){
    String isoDateString = value.toInstant().toString();
    return attributes.put(DATE_PREFIX + name, isoDateString);
  }

  public String addString(String name, String value){
    return (String)attributes.put(STRING_PREFIX + name, value);
  }

  public String addBoolean(String name, Boolean value){
    return (String)attributes.put(BOOLEAN_PREFIX + name, value);
  }

  public String addLong(String name, Boolean value){
    return (String)attributes.put(LONG_PREFIX + name, value);
  }

  public String addDouble(String name, Double value){
    return (String)attributes.put(DOUBLE_PREFIX + name, value);
  }

  public String addInteger(String name, Integer value){
    return (String)attributes.put(INTEGER_PREFIX + name, value);
  }

  public boolean remove(String name){
    Serializable removed = attributes.remove(STRING_PREFIX + name);
    if (removed != null){
      return true;
    }
    removed = attributes.remove(DATE_PREFIX + name);
    if (removed != null){
      return true;
    }
    removed = attributes.remove(DOUBLE_PREFIX + name);
    if (removed != null){
      return true;
    }
    removed = attributes.remove(LONG_PREFIX + name);
    if (removed != null){
      return true;
    }
    removed = attributes.remove(INTEGER_PREFIX + name);
    if (removed != null){
      return true;
    }
    removed = attributes.remove(BOOLEAN_PREFIX + name);
    if (removed != null){
      return true;
    }
    return false;
  }

}
