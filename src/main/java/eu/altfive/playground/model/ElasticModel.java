package eu.altfive.playground.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;
import org.springframework.data.elasticsearch.annotations.ValueConverter;
import org.springframework.data.elasticsearch.core.mapping.PropertyValueConverter;

@Document(indexName = "model")
public class ElasticModel {

  public static final String DATE_PREFIX = "DATE___";
  public static final String STRING_PREFIX = "STRING___";
  public static final String LONG_PREFIX = "LONG___";
  public static final String DOUBLE_PREFIX = "DOUBLE___";
  public static final String INTEGER_PREFIX = "INTEGER___";
  public static final String BOOLEAN_PREFIX = "BOOLEAN___";

  @Id
  @Field(type = FieldType.Keyword)
  private String id;

  @Field(type = FieldType.Flattened)
  @ValueConverter(SpecificAttributesConverter.class)
  private Map<String, Serializable> processVariables;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public Map<String, Serializable> getProcessVariables() {
    return processVariables;
  }

  public void setProcessVariables(Map<String, Serializable> processVariables) {
    this.processVariables = processVariables;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ElasticModel that = (ElasticModel) o;
    return Objects.equals(id, that.id) && Objects.equals(processVariables,
        that.processVariables);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, processVariables);
  }

  static class SpecificAttributesConverter implements PropertyValueConverter {

    public SpecificAttributesConverter() {
    }

    @Override
    public Object write(Object value) {
      Map<String,Serializable> val = (Map<String, Serializable>) value;
      Map<String, String> result = new LinkedHashMap<>();

      val.forEach(
          (k, v) -> {
            switch (v) {
              case String s -> result.put(STRING_PREFIX + k, s);
              case Boolean b -> result.put(BOOLEAN_PREFIX + k, String.valueOf(b));
              case Integer i -> result.put(INTEGER_PREFIX + k, String.valueOf(i));
              case Long l -> result.put(LONG_PREFIX + k, String.valueOf(l));
              case Double d -> result.put(DOUBLE_PREFIX + k, String.valueOf(d));
              case Date date -> result.put(DATE_PREFIX + k, date.toInstant().toString());
              default -> throw new IllegalStateException("Unexpected value: " + v);
            }
          }
      );

      return result;
    }

    @Override
    public Object read(Object value) {
      Map<String,String> val = (Map<String, String>) value;
      Map<String,Serializable> result = new LinkedHashMap<>();

      val.forEach(
          (k, v) -> {
            if (k.startsWith(STRING_PREFIX)){
              result.put(k.substring(STRING_PREFIX.length()), v);
            } else if (k.startsWith(BOOLEAN_PREFIX)){
              result.put(k.substring(BOOLEAN_PREFIX.length()), Boolean.valueOf(v));
            } else if (k.startsWith(INTEGER_PREFIX)) {
              result.put(k.substring(INTEGER_PREFIX.length()), Integer.valueOf(v));
            } else if (k.startsWith(LONG_PREFIX)){
              result.put(k.substring(LONG_PREFIX.length()), Long.valueOf(v));
            } else if (k.startsWith(DOUBLE_PREFIX)){
              result.put(k.substring(DOUBLE_PREFIX.length()), Double.valueOf(v));
            } else if (k.startsWith(DATE_PREFIX)){
              result.put(k.substring(DATE_PREFIX.length()), new Date(Instant.parse(v).toEpochMilli()));
            }
          }
      );

      return result;
    }
  }
}
