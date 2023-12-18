package eu.altfive.playground.projection.model;


import java.util.Date;
import java.util.List;
import org.springframework.data.annotation.Id;
import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Field;
import org.springframework.data.elasticsearch.annotations.FieldType;

@Document(indexName = "model-nested")
public class ElasticModelNested {

  @Id
  @Field(type = FieldType.Keyword)
  private String id;

  @Field(type = FieldType.Keyword)
  private String name;

  @Field(type = FieldType.Nested)
  private List<NestedSpecificAttribute> processVariables;

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public List<NestedSpecificAttribute> getProcessVariables() {
    return processVariables;
  }

  public void setProcessVariables(
      List<NestedSpecificAttribute> processVariables) {
    this.processVariables = processVariables;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public static class NestedSpecificAttribute {

    @Field(type = FieldType.Keyword)
    private String name;

    @Field(type = FieldType.Keyword)
    private String valueString;

    @Field(type = FieldType.Long)
    private Long valueLong;

    @Field(type = FieldType.Date)
    private Date valueDate;

    @Field(type = FieldType.Double)
    private Double valueDouble;

    public String getValueString() {
      return valueString;
    }

    public void setValueString(String valueString) {
      this.valueString = valueString;
    }

    public Long getValueLong() {
      return valueLong;
    }

    public void setValueLong(Long valueLong) {
      this.valueLong = valueLong;
    }

    public Date getValueDate() {
      return valueDate;
    }

    public void setValueDate(Date valueDate) {
      this.valueDate = valueDate;
    }

    public Double getValueDouble() {
      return valueDouble;
    }

    public void setValueDouble(Double valueDouble) {
      this.valueDouble = valueDouble;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

  }

}
