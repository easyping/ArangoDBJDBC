package de.hcbraun.arangodb.jdbc;

public class SchemaVirtual {
  private String collectionName;
  private String collectionVirtual;
  private String columnName;
  private boolean simpleReferences = false;

  public SchemaVirtual(String collectionName, String collectionVirtual, String columnName, boolean simpleReferences) {
    this.collectionName = collectionName;
    this.collectionVirtual = collectionVirtual;
    this.columnName = columnName;
    this.simpleReferences = simpleReferences;
  }

  public String getCollectionName() {
    return collectionName;
  }

  public void setCollectionName(String collectionName) {
    this.collectionName = collectionName;
  }

  public String getCollectionVirtual() {
    return collectionVirtual;
  }

  public void setCollectionVirtual(String collectionVirtual) {
    this.collectionVirtual = collectionVirtual;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public boolean isSimpleReferences() {
    return simpleReferences;
  }

  public void setSimpleReferences(boolean simpleReferences) {
    this.simpleReferences = simpleReferences;
  }
}
