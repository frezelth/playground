package eu.altfive.playground.projection.query;

import java.util.List;

public record SearchCriteria(
    List<SpecificAttributeCriteria> specificAttributeCriteria
) {

  public record SpecificAttributeCriteria(
      String name,
      SpecificAttributeValueType type,
      String stringValue,
      Boolean booleanValue,
      Double numericValueLte,
      Double numericValueGte,

      Long dateValueLte,
      Long dateValueGte
  ){

  }

  public enum SpecificAttributeValueType {

    STRING,
    BOOLEAN,
    INTEGER,
    LONG,
    DOUBLE,
    DATE;

  }

}
