package de.hcbraun.arangodb.jdbc;

import java.util.List;
import java.util.Map;

public interface IChangeMetaData {

  public void setSeparatorStructColumn(String separatorStructColumn);
  public void changeTableList(ArangoDBConnection connection, List<String > tableList);
  public void changeColumnList(ArangoDBConnection connection, List<Map<String, Object>> columnList);
}
