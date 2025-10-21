package de.hcbraun.arangodb.jdbc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ArangoDBListResultSet extends ArangoDBResultSet {

  private final Logger logger = LoggerFactory.getLogger(ArangoDBListResultSet.class);

  private List<Map<String, Object>> lst;
  private Iterator<Map<String, Object>> it = null;
  private Map<String, Object> cur;

  protected ArangoDBListResultSet(List<Map<String, Object>> lst) {
    super(null, null, null);
    this.lst = lst;
    logger.debug("Count-Data-Rows: " + lst.size());
  }

  protected ArangoDBListResultSet(List<Map<String, Object>> lst, ArangoDBResultSetMetaData rsmd) {
    super(null, null, rsmd);
    this.lst = lst;
    try {
      logger.debug("Count-Data-Rows: " + lst.size() + " / Cols-Count: " + rsmd.getColumnCount());
    } catch (SQLException e) {
    }
  }

  @Override
  public boolean next() throws SQLException {
    logger.debug("next");
    if (it == null)
      it = lst.iterator();
    if (it.hasNext())
      cur = it.next();
    else
      cur = null;
    if (cur != null)
      logger.debug("next-Row: " + cur.toString());
    return cur != null;
  }

  @Override
  public boolean first() throws SQLException {
    logger.debug("first");
    it = lst.iterator();
    return it.hasNext();
  }

  @Override
  protected Object getFieldValue(String selectExpression) {
    logger.debug("getFieldValue: " + selectExpression);
    return cur.get(selectExpression);
  }

}
