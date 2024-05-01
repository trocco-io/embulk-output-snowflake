package org.embulk.output.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import java.util.Optional;

public class ToString {
  private final String string;

  public ToString(String string) {
    this.string = string;
  }

  @JsonCreator
  ToString(Optional<JsonNode> option) throws JsonMappingException {
    JsonNode node = option.orElse(NullNode.getInstance());
    if (node.isTextual()) {
      this.string = node.textValue();
    } else if (node.isValueNode()) {
      this.string = node.toString();
    } else {
      throw new JsonMappingException(String.format("Arrays and objects are invalid: '%s'", node));
    }
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof ToString)) {
      return false;
    }
    ToString o = (ToString) obj;
    return string.equals(o.string);
  }

  @Override
  public int hashCode() {
    return string.hashCode();
  }

  @JsonValue
  @Override
  public String toString() {
    return string;
  }
}
