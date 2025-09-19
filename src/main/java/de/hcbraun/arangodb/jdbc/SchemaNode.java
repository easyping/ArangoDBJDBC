package de.hcbraun.arangodb.jdbc;

import java.util.List;

public class SchemaNode {

  private String name;
  private boolean nullable = false;
  private List<Integer> dataType;
  private List<String> references = null;                  // When dataType == Types.STRUCT
  private boolean simpleReferences = false;
  private List<SchemaNode> properties = null;

  private List<Object> enumValues = null;

  SchemaNode(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public boolean isNullable() {
    return nullable;
  }

  public void setNullable(boolean nullable) {
    this.nullable = nullable;
  }

  public List<String> getReferences() {
    return references;
  }

  public void setReferences(List<String> references) {
    this.references = references;
  }

  public List<Object> getEnumValues() {
    return enumValues;
  }

  public void setEnumValues(List<Object> enumValues) {
    this.enumValues = enumValues;
  }

  public List<Integer> getDataType() {
    return dataType;
  }

  public void setDataType(List<Integer> dataType) {
    this.dataType = dataType;
  }

  public List<SchemaNode> getProperties() {
    return properties;
  }

  public void setProperties(List<SchemaNode> properties) {
    this.properties = properties;
  }

  public boolean isSimpleReferences() {
    return simpleReferences;
  }

  public void setSimpleReferences(boolean simpleReferences) {
    this.simpleReferences = simpleReferences;
  }
}
