package de.hcbraun.arangodb.jdbc;

import java.util.List;
import java.util.Map;

public class CollectionSchema {

  private long nextRefresh = 0;
  private String collection;
  private String aliasName = null;                          // Connection-Property collectionAlias

  private List<SchemaNode> properties;
  private Map<String, SchemaReference> references = null;
  private Map<String, SchemaDatatype> datatypes = null;

  CollectionSchema(String collection) {
    this.collection = collection;
  }

  public long getNextRefresh() {
    return nextRefresh;
  }

  public void setNextRefresh(long nextRefresh) {
    this.nextRefresh = nextRefresh;
  }

  public String getCollection() {
    return collection;
  }

  public String getAliasName() {
    return aliasName;
  }

  public void setAliasName(String aliasName) {
    this.aliasName = aliasName;
  }

  public List<SchemaNode> getProperties() {
    return properties;
  }

  public void setProperties(List<SchemaNode> properties) {
    this.properties = properties;
  }

  public Map<String, SchemaReference> getReferences() {
    return references;
  }

  public void setReferences(Map<String, SchemaReference> references) {
    this.references = references;
  }

  public Map<String, SchemaDatatype> getDatatypes() {
    return datatypes;
  }

  public void setDatatypes(Map<String, SchemaDatatype> datatypes) {
    this.datatypes = datatypes;
  }
}
