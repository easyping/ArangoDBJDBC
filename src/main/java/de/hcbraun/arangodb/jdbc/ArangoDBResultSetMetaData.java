package de.hcbraun.arangodb.jdbc;

import com.arangodb.entity.BaseDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.*;
import java.sql.Date;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ArangoDBResultSetMetaData implements ResultSetMetaData {

  private final Logger logger = LoggerFactory.getLogger(ArangoDBResultSetMetaData.class);

  private final static Pattern commentsPattern = Pattern.compile("(//.*?$)|(/\\*.*?\\*/)", Pattern.MULTILINE | Pattern.DOTALL);
  private final static String[] DTYP_SHORT = {"s", "i", "b", "d", "f", "bd", "dt", "t", "ts"};
  private final static String[] DTYP_LONG = {"string", "integer", "boolean", "double", "float", "bigdecimal",
    "date", "time", "timestamp"};
  private final static int[] DTYP_TYPES = {Types.VARCHAR, Types.INTEGER, Types.BOOLEAN, Types.DOUBLE, Types.FLOAT,
    Types.DECIMAL, Types.DATE, Types.TIME, Types.TIMESTAMP};
  private final static String[] DTYP_TYPENAMES = {"NVARCHAR", "INTEGER", "BOOLEAN", "DOUBLE", "FLOAT",
    "DECIMAL", "DATE", "TIME", "TIMESTAMP"};
  private final static String[] DTYP_CLASSNAME = {String.class.getName(), Integer.class.getName(), Boolean.class.getName(), Double.class.getName(),
    Float.class.getName(), BigDecimal.class.getName(), Date.class.getName(), Time.class.getName(), Timestamp.class.getName()};

  private ArrayList<ColInfo> cols;

  protected ArangoDBResultSetMetaData(String sqlComments) {
    parseCommentsForColumns(sqlComments);
  }

  protected ArangoDBResultSetMetaData(BaseDocument doc) {
    cols = new ArrayList<>();
    if (doc.getId() != null)
      cols.add(new ColInfo("_id", "NVARCHAR", Types.VARCHAR, String.class.getName()));
    if (doc.getKey() != null)
      cols.add(new ColInfo("_key", "NVARCHAR", Types.VARCHAR, String.class.getName()));
    analyseColMap(doc.getProperties(), "");
  }

  protected ArangoDBResultSetMetaData(ArrayList<ColInfo> lstColumns) {
    cols = lstColumns;
  }

  private void analyseColMap(Map<String, Object> map, String prefix) {
    for (String key : map.keySet()) {
      Object obj = map.get(key);
      String colName = prefix + key;
      if (obj == null)
        cols.add(new ColInfo(colName, "", Types.VARCHAR, String.class.getName()));
      else if (obj instanceof String)
        cols.add(new ColInfo(colName, "NVARCHAR", Types.VARCHAR, String.class.getName()));
      else if (obj instanceof Number)
        cols.add(new ColInfo(colName, "DOUBLE", Types.DOUBLE, Double.class.getName()));
      else if (obj instanceof Boolean)
        cols.add(new ColInfo(colName, "BOOLEAN", Types.BOOLEAN, Boolean.class.getName()));
      else if (obj instanceof List) {
        Object uObj = ((List<?>) obj).get(0);
        if (uObj == null)
          cols.add(new ColInfo(colName, "", Types.VARCHAR, String.class.getName()));
        else if (uObj instanceof String)
          cols.add(new ColInfo(colName, "NVARCHAR", Types.VARCHAR, String.class.getName()));
        else if (uObj instanceof Number)
          cols.add(new ColInfo(colName, "DOUBLE", Types.DOUBLE, Double.class.getName()));
        else if (uObj instanceof Boolean)
          cols.add(new ColInfo(colName, "BOOLEAN", Types.BOOLEAN, Boolean.class.getName()));
        else if (uObj instanceof Map)
          analyseColMap((Map<String, Object>) uObj, colName + ".");
      } else if (obj instanceof Map)
        analyseColMap((Map<String, Object>) obj, colName + ".");
    }
  }

  protected ArrayList<ColInfo> getColInfo() {
    return cols;
  }

  @Override
  public int getColumnCount() throws SQLException {
    logger.debug("getColumnCount: " + cols.size());
    return cols.size();
  }

  @Override
  public boolean isAutoIncrement(int i) throws SQLException {
    logger.debug("isAutoIncrement");
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isCaseSensitive(int i) throws SQLException {
    logger.debug("isCaseSensitive");
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isSearchable(int i) throws SQLException {
    logger.debug("isSearchable");
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isCurrency(int i) throws SQLException {
    logger.debug("isCurrency");
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int isNullable(int i) throws SQLException {
    logger.debug("isNullable");
    // TODO Auto-generated method stub
    return columnNullableUnknown;
  }

  @Override
  public boolean isSigned(int i) throws SQLException {
    logger.debug("isSigned");
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int getColumnDisplaySize(int i) throws SQLException {
    logger.debug("getColumnDisplaySize");
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String getColumnLabel(int i) throws SQLException {
    logger.debug("getColumnLabel");
    return cols.get(i - 1).name;
  }

  @Override
  public String getColumnName(int i) throws SQLException {
    logger.debug("getColumnName");
    return cols.get(i - 1).name;
  }

  @Override
  public String getSchemaName(int i) throws SQLException {
    logger.debug("getSchemaName");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getPrecision(int i) throws SQLException {
    logger.debug("getPrecision");
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getScale(int i) throws SQLException {
    logger.debug("getScale");
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String getTableName(int i) throws SQLException {
    logger.debug("getTableName");
    return cols.get(i - 1).tabName;
  }

  @Override
  public String getCatalogName(int i) throws SQLException {
    logger.debug("getCatalogName");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int getColumnType(int i) throws SQLException {
    logger.debug("getColumnType");
    return cols.get(i - 1).type;
  }

  @Override
  public String getColumnTypeName(int i) throws SQLException {
    logger.debug("getColumnTypeName");
    return cols.get(i - 1).typeName;
  }

  @Override
  public boolean isReadOnly(int i) throws SQLException {
    logger.debug("isReadOnly");
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isWritable(int i) throws SQLException {
    logger.debug("isWritable");
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean isDefinitelyWritable(int i) throws SQLException {
    logger.debug("isDefinitelyWritable");
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public String getColumnClassName(int i) throws SQLException {
    logger.debug("getColumnClassName");
    return cols.get(i - 1).className;
  }

  @Override
  public <T> T unwrap(Class<T> aClass) throws SQLException {
    logger.debug("unwrap");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> aClass) throws SQLException {
    logger.debug("isWrapperFor");
    // TODO Auto-generated method stub
    return false;
  }

  private void parseCommentsForColumns(String comments) {
    logger.debug("parseCommentsForColumns: " + comments);
    Matcher matcher = commentsPattern.matcher(comments);
    cols = new ArrayList<>();
    while (matcher.find()) {
      String comment = matcher.group();
      logger.debug("comment: " + comment);
      if (comment.indexOf("cols:") > 0) {
        logger.debug("cols");
        if (comment.trim().endsWith("*/"))
          comment = comment.trim().substring(0, comment.trim().length() - 2);
        String[] columns = comment.substring(comment.indexOf("cols:") + 5).split(",");
        for (String c : columns) {
          logger.debug(c);
          String[] nt = c.trim().split(":");
          int typ = -1, i = 0;
          String clNam = null, typNam = null;
          String nam = nt[1].toLowerCase();
          for (String t : DTYP_SHORT) {
            if (t.equals(nam)) {
              typ = DTYP_TYPES[i];
              typNam = DTYP_TYPENAMES[i];
              clNam = DTYP_CLASSNAME[i];
              break;
            }
            i++;
          }
          if (typ == -1) {
            i = 0;
            for (String t : DTYP_LONG) {
              if (t.equals(nam)) {
                typ = DTYP_TYPES[i];
                typNam = DTYP_TYPENAMES[i];
                clNam = DTYP_CLASSNAME[i];
                break;
              }
              i++;
            }
            if (typ == -1) {
              typ = Types.VARCHAR;
              typNam = DTYP_TYPENAMES[0];
              clNam = DTYP_CLASSNAME[0];
            }
          }
          cols.add(new ColInfo(nt[0], typNam, typ, clNam));
        }
      }
    }
    logger.debug("parseCommentsForColumns founds cols: " + cols.size());
  }

}
