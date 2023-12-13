package eu.altfive.playground;

import java.io.Serializable;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

public class TestSwitch {

  private static final String DATE_PREFIX = "DATE___";
  private static final String STRING_PREFIX = "STRING___";
  private static final String LONG_PREFIX = "LONG___";
  private static final String DOUBLE_PREFIX = "DOUBLE___";
  private static final String INTEGER_PREFIX = "INTEGER___";
  private static final String BOOLEAN_PREFIX = "BOOLEAN___";

  @Test
  void testSwitch(){
    Map<String, Serializable> val = Map.of(
        "key-string", "value-string",
        "key-date", new Date()
    );
    Map<String, String> result = new LinkedHashMap<>();

    Map.Entry<String, Serializable> entry = val.entrySet().iterator().next();
    String s1 = switch (entry.getValue()) {
      case String s -> result.put(STRING_PREFIX + entry.getKey(), s);
      case Boolean b -> result.put(BOOLEAN_PREFIX + entry.getKey(), String.valueOf(b));
      case Integer i -> result.put(INTEGER_PREFIX + entry.getKey(), String.valueOf(i));
      case Long l -> result.put(LONG_PREFIX + entry.getKey(), String.valueOf(l));
      case Double d -> result.put(DOUBLE_PREFIX + entry.getKey(), String.valueOf(d));
      case Date date -> result.put(DATE_PREFIX + entry.getKey(), date.toInstant().toString());
      default -> throw new IllegalStateException("Unexpected value: " + entry.getValue());
    };
    System.out.println(s1);
    System.out.println(result);
  }

}
