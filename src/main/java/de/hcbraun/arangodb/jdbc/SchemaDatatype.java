package de.hcbraun.arangodb.jdbc;

public class SchemaDatatype {
  private String name;
  private int type;

  SchemaDatatype(String name, int type) {
    this.name = name;
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public int getType() {
    return type;
  }
}
