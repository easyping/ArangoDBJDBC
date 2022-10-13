package de.hcbraun.arangodb.jdbc;

import java.util.Map;

public class QueryInfo {
  String aql = null;
  ArangoDBResultSetMetaData rsmd = null;
  Map<String, Object> parameters = null;

  public QueryInfo() {
  }
}
