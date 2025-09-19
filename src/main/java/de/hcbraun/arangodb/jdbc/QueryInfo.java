package de.hcbraun.arangodb.jdbc;

import java.util.Map;

public class QueryInfo {
  String aql = null;
  ArangoDBResultSetMetaData rsmd = null;
  Map<String, Object> parameters = null;

  public QueryInfo() {
  }

  public String getAql() {
    return aql;
  }

  public void setAql(String aql) {
    this.aql = aql;
  }

  public ArangoDBResultSetMetaData getRsmd() {
    return rsmd;
  }

  public void setRsmd(ArangoDBResultSetMetaData rsmd) {
    this.rsmd = rsmd;
  }

  public Map<String, Object> getParameters() {
    return parameters;
  }

  public void setParameters(Map<String, Object> parameters) {
    this.parameters = parameters;
  }
}
