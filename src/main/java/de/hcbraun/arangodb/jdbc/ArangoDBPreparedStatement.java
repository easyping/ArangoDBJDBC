package de.hcbraun.arangodb.jdbc;

import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;
import com.arangodb.model.AqlQueryOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ArangoDBPreparedStatement extends ArangoDBStatement implements PreparedStatement {

  private final Logger logger = LoggerFactory.getLogger(ArangoDBPreparedStatement.class);

  private String sql = null;
  private ArrayList<String> para = new ArrayList<>();
  private Map<String, Object> mapBuild = new LinkedHashMap<>();
  private ArrayList<Map<String, Object>> lstBatch = null;

  protected ArangoDBPreparedStatement(ArangoDBConnection connection, String sql) {
    super(connection);

    Pattern pattern = Pattern.compile("@[\\w]+");
    Matcher matcher = pattern.matcher(sql);
    while (matcher.find())
      para.add(matcher.group().substring(1));

    if (sql.contains("?")) {
      String[] part = sql.split("\\?");
      StringBuilder nsql = new StringBuilder();
      int p = 1;
      for (int i = 0; i < part.length - 1; i++) {
        String s = part[i];
        nsql.append(s);
        s = "para" + p++;
        para.add(s);
        nsql.append("@");
        nsql.append(s);
      }
      nsql.append(part[part.length - 1]);
      sql = nsql.toString();
      logger.debug("Neu-Sql: " + sql);
    }
    this.sql = sql;
  }

  @Override
  public ResultSet executeQuery() throws SQLException {
    logger.debug("executeQuery " + sql);
    try {
      QueryInfo qi = getAQL(sql, mapBuild);
      return new ArangoDBResultSet(database.query(qi.aql, BaseDocument.class, qi.parameters), this, qi.rsmd);
    } catch (ArangoDBException e) {
      e.printStackTrace();
      throw new SQLException(e.getErrorMessage());
    }
  }

  @Override
  public int executeUpdate() throws SQLException {
    logger.debug("executeUpdate " + sql);
    try {
      QueryInfo qi = getAQL(sql, mapBuild);
      ArangoCursor<BaseDocument> cursor = database.query(qi.aql, BaseDocument.class, qi.parameters);
      if (cursor != null) {
        int a = 0;
        while (cursor.hasNext()) {
          a++;
          cursor.next();
        }
        return a;
      }
    } catch (ArangoDBException e) {
      System.err.println("Error in sql " + sql + " with parameters " + mapBuild.toString());
      e.printStackTrace();
      throw new SQLException(e.getErrorMessage());
    }
    return 0;
  }

  private String getParameter(int parameterIndex) {
    if (para != null && para.size() > 0)
      return para.get(parameterIndex - 1);
    return "p" + parameterIndex;
  }

  @Override
  public void setNull(int parameterIndex, int sqlType) throws SQLException {
    mapBuild.put(getParameter(parameterIndex), null);
  }

  @Override
  public void setBoolean(int parameterIndex, boolean x) throws SQLException {
    mapBuild.put(getParameter(parameterIndex), x ? "true" : "false");
  }

  @Override
  public void setByte(int parameterIndex, byte x) throws SQLException {
    mapBuild.put(getParameter(parameterIndex), x);
  }

  @Override
  public void setShort(int parameterIndex, short x) throws SQLException {
    mapBuild.put(getParameter(parameterIndex), x);
  }

  @Override
  public void setInt(int parameterIndex, int x) throws SQLException {
    mapBuild.put(getParameter(parameterIndex), x);
  }

  @Override
  public void setLong(int parameterIndex, long x) throws SQLException {
    mapBuild.put(getParameter(parameterIndex), x);
  }

  @Override
  public void setFloat(int parameterIndex, float x) throws SQLException {
    mapBuild.put(getParameter(parameterIndex), x);
  }

  @Override
  public void setDouble(int parameterIndex, double x) throws SQLException {
    mapBuild.put(getParameter(parameterIndex), x);
  }

  @Override
  public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
    mapBuild.put(getParameter(parameterIndex), x.doubleValue());
  }

  @Override
  public void setString(int parameterIndex, String x) throws SQLException {
    mapBuild.put(getParameter(parameterIndex), x);
  }

  @Override
  public void setBytes(int parameterIndex, byte[] x) throws SQLException {
    mapBuild.put(getParameter(parameterIndex), x);
  }

  @Override
  public void setDate(int parameterIndex, Date x) throws SQLException {
    mapBuild.put(getParameter(parameterIndex), x);
  }

  @Override
  public void setTime(int parameterIndex, Time x) throws SQLException {
    mapBuild.put(getParameter(parameterIndex), x);
  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
    mapBuild.put(getParameter(parameterIndex), x);
  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void clearParameters() throws SQLException {
    mapBuild = new LinkedHashMap<>();
  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
    mapBuild.put(getParameter(parameterIndex), x);
  }

  @Override
  public void setObject(int parameterIndex, Object x) throws SQLException {
    mapBuild.put(getParameter(parameterIndex), x);
  }

  @Override
  public boolean execute() throws SQLException {
    try {
      QueryInfo qi = getAQL(sql, mapBuild);
      ArangoCursor<BaseDocument> cursor = database.query(qi.aql, BaseDocument.class, qi.parameters);
      if (cursor != null && cursor.hasNext())
        return true;
    } catch (ArangoDBException e) {
      System.err.println("Error in sql " + sql + " with parameters " + mapBuild.toString());
      e.printStackTrace();
      throw new SQLException(e.getErrorMessage());
    }
    return false;
  }

  @Override
  public void addBatch() throws SQLException {
    if (lstBatch == null)
      lstBatch = new ArrayList<>();
    lstBatch.add(mapBuild);
    mapBuild = new LinkedHashMap<>();
  }

  @Override
  public int[] executeBatch() throws SQLException {
    logger.debug("executeBatch " + sql);
    try {
      AqlQueryOptions aqo = new AqlQueryOptions();
      aqo.count(true);
      ArrayList<Integer> ergLst = new ArrayList<>();
      for (Map mb : lstBatch) {
        QueryInfo qi = getAQL(sql, mb);
        ArangoCursor<BaseDocument> erg = database.query(qi.aql, BaseDocument.class, qi.parameters, aqo);
        ergLst.add(erg.getCount());
      }
      lstBatch = null;
      int[] l = new int[ergLst.size()];
      for (int i = 0; i < ergLst.size(); i++)
        l[i] = ergLst.get(i);
      return l;
    } catch (ArangoDBException e) {
      e.printStackTrace();
      throw new SQLException(e.getErrorMessage());
    }
//    return null;
  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, int length)
          throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setRef(int parameterIndex, Ref x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setBlob(int parameterIndex, Blob x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setClob(int parameterIndex, Clob x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setArray(int parameterIndex, Array x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setURL(int parameterIndex, URL x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public ParameterMetaData getParameterMetaData() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void setRowId(int parameterIndex, RowId x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setNString(int parameterIndex, String value) throws SQLException {
    setString(parameterIndex, value);
  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setNClob(int parameterIndex, NClob value) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setClob(int parameterIndex, Reader reader) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
    // TODO Auto-generated method stub

  }

  @Override
  public void setNClob(int parameterIndex, Reader reader) throws SQLException {
    // TODO Auto-generated method stub

  }

}
