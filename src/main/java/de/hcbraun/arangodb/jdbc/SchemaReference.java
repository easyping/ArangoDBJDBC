package de.hcbraun.arangodb.jdbc;

import java.util.List;

public class SchemaReference {

  private String name;

  private List<SchemaNode> properties;

  SchemaReference(String name) {
    this.name = name;
  }

  SchemaReference(String name, List<SchemaNode> properties) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public List<SchemaNode> getProperties() {
    return properties;
  }

  public void setProperties(List<SchemaNode> properties) {
    this.properties = properties;
  }
}
