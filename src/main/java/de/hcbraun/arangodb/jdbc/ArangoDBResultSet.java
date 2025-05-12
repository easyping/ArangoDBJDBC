package de.hcbraun.arangodb.jdbc;

import com.arangodb.ArangoCursor;
import com.arangodb.entity.BaseDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ArangoDBResultSet implements ResultSet {

  private final static String PROPERTY_SEPARATOR = ".";
  private final static String ARRAY_LEFT = "[";
  private final static String ARRAY_RIGHT = "]";

  private final Logger logger = LoggerFactory.getLogger(ArangoDBResultSet.class);

  private ArangoCursor<BaseDocument> cursor;
  private final ArangoDBStatement stat;
  private ArangoDBResultSetMetaData metaData = null;
  private ArrayList<ColInfo> lstCols = null;

  private BaseDocument curDoc = null;
  private boolean first = true, last = false, wasNull = false;
  private int row = 0;

  private final DateFormat dfJs = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
  private final DateFormat dfSD = new SimpleDateFormat("yyyy-MM-dd");
  private final DateFormat dfST = new SimpleDateFormat("HH:mm:ss");
  private final DateFormat dfTS = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  protected ArangoDBResultSet(ArangoCursor<BaseDocument> cursor, ArangoDBStatement stat, ArangoDBResultSetMetaData metaData) {
    this.cursor = cursor;
    this.stat = stat;
    logger.debug("ResultSet: " + (cursor != null));
    this.metaData = metaData;
    if (metaData != null)
      this.lstCols = metaData.getColInfo();
  }

  @Override
  public boolean next() throws SQLException {
    if (cursor != null && cursor.hasNext()) {
      first = curDoc == null;
      if (first)
        curDoc = cursor.stream().findFirst().orElse(null);
      else
        curDoc = cursor.next();
      row++;
    } else
      curDoc = null;
    if (cursor != null)
      last = !cursor.hasNext();
    else
      last = true;
    logger.debug("Next: " + (curDoc != null ? curDoc.getId() : "null"));
    return curDoc != null;
  }

  @Override
  public void close() throws SQLException {
    logger.debug("close");
    if (cursor != null) {
      try {
        cursor.close();
      } catch (IOException e) {
        e.printStackTrace();
        throw new SQLException(e.getMessage());
      }
    }
    cursor = null;
  }

  @Override
  public boolean wasNull() throws SQLException {
    return wasNull;
  }

  @Override
  public String getString(int i) throws SQLException {
    logger.debug("getString - index: " + i);
    wasNull = false;
    return lstCols != null ? getString(lstCols.get(i - 1).name) : null;
  }

  @Override
  public boolean getBoolean(int i) throws SQLException {
    logger.debug("getBoolean - index: " + i);
    wasNull = false;
    return lstCols != null && getBoolean(lstCols.get(i - 1).name);
  }

  @Override
  public byte getByte(int i) throws SQLException {
    logger.debug("getByte - index");
    return 0;
  }

  @Override
  public short getShort(int i) throws SQLException {
    logger.debug("getShort - index: " + i);
    wasNull = false;
    return lstCols != null ? getShort(lstCols.get(i - 1).name) : 0;
  }

  @Override
  public int getInt(int i) throws SQLException {
    logger.debug("getInt - index: " + i);
    wasNull = false;
    return lstCols != null ? getInt(lstCols.get(i - 1).name) : 0;
  }

  @Override
  public long getLong(int i) throws SQLException {
    logger.debug("getLong - index: " + i);
    wasNull = false;
    return lstCols != null ? getLong(lstCols.get(i - 1).name) : 0;
  }

  @Override
  public float getFloat(int i) throws SQLException {
    logger.debug("getFloat - index: " + i);
    wasNull = false;
    return lstCols != null ? getFloat(lstCols.get(i - 1).name) : 0;
  }

  @Override
  public double getDouble(int i) throws SQLException {
    logger.debug("getDouble - index: " + i);
    wasNull = false;
    return lstCols != null ? getDouble(lstCols.get(i - 1).name) : 0;
  }

  @Override
  public BigDecimal getBigDecimal(int i, int i1) throws SQLException {
    logger.debug("getBigDecimal - index/scale: " + i);
    wasNull = false;
    return lstCols != null ? getBigDecimal(lstCols.get(i - 1).name) : null;
  }

  @Override
  public byte[] getBytes(int i) throws SQLException {
    logger.debug("getBytes - index");
    wasNull = false;
    return new byte[0];
  }

  @Override
  public Date getDate(int i) throws SQLException {
    logger.debug("getDate - index: " + i);
    wasNull = false;
    return lstCols != null ? getDate(lstCols.get(i - 1).name) : null;
  }

  @Override
  public Time getTime(int i) throws SQLException {
    logger.debug("getTime - index: " + i);
    wasNull = false;
    return lstCols != null ? getTime(lstCols.get(i - 1).name) : null;
  }

  @Override
  public Timestamp getTimestamp(int i) throws SQLException {
    logger.debug("getTimestamp - index: " + i);
    wasNull = false;
    return lstCols != null ? getTimestamp(lstCols.get(i - 1).name) : null;
  }

  @Override
  public InputStream getAsciiStream(int i) throws SQLException {
    logger.debug("getAsciiStream - index");
    return null;
  }

  @Override
  public InputStream getUnicodeStream(int i) throws SQLException {
    logger.debug("getUnicodeStream - index");
    return null;
  }

  @Override
  public InputStream getBinaryStream(int i) throws SQLException {
    logger.debug("getBinaryStream - index");
    return null;
  }

  @Override
  public String getString(String s) throws SQLException {
    Object obj = getFieldValue(s);
    wasNull = obj == null;
    return obj == null ? null : obj.toString();
  }

  @Override
  public boolean getBoolean(String s) throws SQLException {
    Object obj = getFieldValue(s);
    if (obj instanceof Boolean)
      return (Boolean) obj;
    wasNull = obj == null;
    return obj != null && "true".equals(obj.toString().toLowerCase());
  }

  @Override
  public byte getByte(String s) throws SQLException {
    Object obj = getFieldValue(s);
    if (obj instanceof Number)
      return ((Number) obj).byteValue();
    wasNull = obj == null;
    return obj == null ? 0 : Byte.parseByte(obj.toString());
  }

  @Override
  public short getShort(String s) throws SQLException {
    Object obj = getFieldValue(s);
    if (obj instanceof Number)
      return ((Number) obj).shortValue();
    wasNull = obj == null;
    return obj == null ? 0 : Short.parseShort(obj.toString());
  }

  @Override
  public int getInt(String s) throws SQLException {
    Object obj = getFieldValue(s);
    if (obj instanceof Number)
      return ((Number) obj).intValue();
    wasNull = obj == null;
    return obj == null ? 0 : Integer.parseInt(obj.toString());
  }

  @Override
  public long getLong(String s) throws SQLException {
    Object obj = getFieldValue(s);
    if (obj instanceof Number)
      return ((Number) obj).longValue();
    wasNull = obj == null;
    return obj == null ? 0 : Long.parseLong(obj.toString());
  }

  @Override
  public float getFloat(String s) throws SQLException {
    Object obj = getFieldValue(s);
    if (obj instanceof Number)
      return ((Number) obj).floatValue();
    wasNull = obj == null;
    return obj == null ? 0 : Float.parseFloat(obj.toString());
  }

  @Override
  public double getDouble(String s) throws SQLException {
    Object obj = getFieldValue(s);
    if (obj instanceof Number)
      return ((Number) obj).doubleValue();
    wasNull = obj == null;
    return obj == null ? 0 : Double.parseDouble(obj.toString());
  }

  @Override
  public BigDecimal getBigDecimal(String s, int i) throws SQLException {
    logger.debug("getBigDecimal - colname/scale");
    return null;
  }

  @Override
  public byte[] getBytes(String s) throws SQLException {
    logger.debug("getBytes - colname");
    return new byte[0];
  }

  @Override
  public Date getDate(String s) throws SQLException {
    Object obj = getFieldValue(s);
    wasNull = obj == null;
    if (!wasNull) {
      String dat = obj.toString();
      Date d = null;
      if (dat.indexOf("T") > 0) {
        try {
          dat = dat.replace('T', ' ');
          d = new Date(dfJs.parse(dat).getTime());
        } catch (ParseException e) {
          e.printStackTrace();
        }
      } else {
        try {
          d = new Date(dfSD.parse(dat).getTime());
        } catch (ParseException e) {
          e.printStackTrace();
        }
      }
      return d;
    }
    return null;
  }

  @Override
  public Time getTime(String s) throws SQLException {
    Object obj = getFieldValue(s);
    wasNull = obj == null;
    if (!wasNull) {
      String dat = obj.toString();
      Time d = null;
      if (dat.indexOf("T") > 0) {
        try {
          dat = dat.replace('T', ' ');
          d = new Time(dfJs.parse(dat).getTime());
        } catch (ParseException e) {
          e.printStackTrace();
        }
      } else {
        try {
          d = new Time(dfST.parse(dat).getTime());
        } catch (ParseException e) {
          e.printStackTrace();
        }
      }
      return d;
    }
    return null;
  }

  @Override
  public Timestamp getTimestamp(String s) throws SQLException {
    Object obj = getFieldValue(s);
    wasNull = obj == null;
    if (!wasNull) {
      String dat = obj.toString();
      Timestamp d = null;
      if (dat.indexOf("T") > 0) {
        try {
          dat = dat.replace('T', ' ');
          d = new Timestamp(dfJs.parse(dat).getTime());
        } catch (ParseException e) {
          e.printStackTrace();
        }
      } else {
        try {
          d = new Timestamp(dfTS.parse(dat).getTime());
        } catch (ParseException e) {
          e.printStackTrace();
        }
      }
      return d;
    }
    return null;
  }

  @Override
  public InputStream getAsciiStream(String s) throws SQLException {
    logger.debug("getAsciiStream");
    return null;
  }

  @Override
  public InputStream getUnicodeStream(String s) throws SQLException {
    logger.debug("getUnicodeStream");
    return null;
  }

  @Override
  public InputStream getBinaryStream(String s) throws SQLException {
    logger.debug("getBinaryStream");
    return null;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    logger.debug("getWarnings");
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
    logger.debug("clearWarnings");
  }

  @Override
  public String getCursorName() throws SQLException {
    return cursor.getId();
  }

  @Override
  public ResultSetMetaData getMetaData() throws SQLException {
    logger.debug("getMetaData");
    return metaData;
  }

  @Override
  public Object getObject(int i) throws SQLException {
    logger.debug("getObject - index: " + i);
    wasNull = false;
    return lstCols != null ? getObject(lstCols.get(i - 1).name) : null;
  }

  @Override
  public Object getObject(String s) throws SQLException {
    return getFieldValue(s);
  }

  @Override
  public int findColumn(String s) throws SQLException {
    logger.debug("findColumn - colname: " + s);
    if(lstCols != null) {
      int i = 0;
      for(ColInfo ci : lstCols) {
        i++;
        if(s.equals(ci.name))
          return i;
      }
    }
    return 0;
  }

  @Override
  public Reader getCharacterStream(int i) throws SQLException {
    logger.debug("getCharacterStream - index");
    return null;
  }

  @Override
  public Reader getCharacterStream(String s) throws SQLException {
    logger.debug("getCharacterStream - colname");
    return null;
  }

  @Override
  public BigDecimal getBigDecimal(int i) throws SQLException {
    logger.debug("getBigDecimal - index: " + i);
    return getBigDecimal(lstCols.get(i - 1).name);
  }

  @Override
  public BigDecimal getBigDecimal(String s) throws SQLException {
    Object obj = getFieldValue(s);
    if (obj instanceof BigDecimal)
      return (BigDecimal) obj;
    else if (obj instanceof Number)
      return BigDecimal.valueOf(((Number) obj).doubleValue());
    wasNull = obj == null;
    return obj == null ? null : new BigDecimal(obj.toString());
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    logger.debug("isBeforeFirst");
    return curDoc == null && first;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    logger.debug("isAfterLast");
    return curDoc == null && last;
  }

  @Override
  public boolean isFirst() throws SQLException {
    logger.debug("isFirst");
    return first;
  }

  @Override
  public boolean isLast() throws SQLException {
    logger.debug("isLast");
    return last;
  }

  @Override
  public void beforeFirst() throws SQLException {
    logger.debug("beforeFirst");
  }

  @Override
  public void afterLast() throws SQLException {
    logger.debug("afterLast");
  }

  @Override
  public boolean first() throws SQLException {
    row = 0;
    if (cursor != null) {
      curDoc = cursor.stream().findFirst().orElse(null);
      first = curDoc != null;
      last = false;
      if (first)
        row++;
    } else {
      curDoc = null;
      last = true;
    }
    logger.debug("First: " + (curDoc != null ? curDoc.getId() : "null"));
    return curDoc != null;
  }

  @Override
  public boolean last() throws SQLException {
    logger.debug("last");
    return false;
  }

  @Override
  public int getRow() throws SQLException {
    logger.debug("getRow");
    return row;
  }

  @Override
  public boolean absolute(int i) throws SQLException {
    logger.debug("absolute");
    return false;
  }

  @Override
  public boolean relative(int i) throws SQLException {
    logger.debug("relative");
    return false;
  }

  @Override
  public boolean previous() throws SQLException {
    logger.debug("previous");
    return false;
  }

  @Override
  public void setFetchDirection(int i) throws SQLException {
    logger.debug("setFetchDirection");
    // TODO Auto-generated method stub
  }

  @Override
  public int getFetchDirection() throws SQLException {
    logger.debug("getFetchDirection");
    return FETCH_FORWARD;
  }

  @Override
  public void setFetchSize(int i) throws SQLException {
    logger.debug("setFetchSize");
    // TODO Auto-generated method stub
  }

  @Override
  public int getFetchSize() throws SQLException {
    return 1;
  }

  @Override
  public int getType() throws SQLException {
    logger.debug("getType");
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getConcurrency() throws SQLException {
    logger.debug("getConcurrency");
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean rowUpdated() throws SQLException {
    logger.debug("rowUpdated");
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean rowInserted() throws SQLException {
    logger.debug("rowInserted");
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public boolean rowDeleted() throws SQLException {
    logger.debug("rowDeleted");
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void updateNull(int i) throws SQLException {
    logger.debug("updateNull");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateBoolean(int i, boolean b) throws SQLException {
    logger.debug("updateBoolean");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateByte(int i, byte b) throws SQLException {
    logger.debug("updateByte");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateShort(int i, short i1) throws SQLException {
    logger.debug("updateShort");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateInt(int i, int i1) throws SQLException {
    logger.debug("updateInt");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateLong(int i, long l) throws SQLException {
    logger.debug("updateLong");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateFloat(int i, float v) throws SQLException {
    logger.debug("updateFloat");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateDouble(int i, double v) throws SQLException {
    logger.debug("updateDouble");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateBigDecimal(int i, BigDecimal bigDecimal) throws SQLException {
    logger.debug("updateBigDecimal");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateString(int i, String s) throws SQLException {
    logger.debug("updateString");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateBytes(int i, byte[] bytes) throws SQLException {
    logger.debug("updateBytes");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateDate(int i, Date date) throws SQLException {
    logger.debug("updateDate");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateTime(int i, Time time) throws SQLException {
    logger.debug("updateTime");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateTimestamp(int i, Timestamp timestamp) throws SQLException {
    logger.debug("updateTimestamp");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateAsciiStream(int i, InputStream inputStream, int i1) throws SQLException {
    logger.debug("updateAsciiStream");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateBinaryStream(int i, InputStream inputStream, int i1) throws SQLException {
    logger.debug("updateBinaryStream");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateCharacterStream(int i, Reader reader, int i1) throws SQLException {
    logger.debug("updateCharacterStream");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateObject(int i, Object o, int i1) throws SQLException {
    logger.debug("updateObject");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateObject(int i, Object o) throws SQLException {
    logger.debug("updateObject");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateNull(String s) throws SQLException {
    logger.debug("updateNull");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateBoolean(String s, boolean b) throws SQLException {
    logger.debug("updateBoolean");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateByte(String s, byte b) throws SQLException {
    logger.debug("updateByte");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateShort(String s, short i) throws SQLException {
    logger.debug("updateShort");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateInt(String s, int i) throws SQLException {
    logger.debug("updateInt");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateLong(String s, long l) throws SQLException {
    logger.debug("updateInt");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateFloat(String s, float v) throws SQLException {
    logger.debug("updateFloat");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateDouble(String s, double v) throws SQLException {
    logger.debug("updateDouble");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateBigDecimal(String s, BigDecimal bigDecimal) throws SQLException {
    logger.debug("updateBigDecimal");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateString(String s, String s1) throws SQLException {
    logger.debug("updateString");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateBytes(String s, byte[] bytes) throws SQLException {
    logger.debug("updateBytes");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateDate(String s, Date date) throws SQLException {
    logger.debug("updateDate");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateTime(String s, Time time) throws SQLException {
    logger.debug("updateTime");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateTimestamp(String s, Timestamp timestamp) throws SQLException {
    logger.debug("updateTimestamp");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateAsciiStream(String s, InputStream inputStream, int i) throws SQLException {
    logger.debug("updateAsciiStream");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateBinaryStream(String s, InputStream inputStream, int i) throws SQLException {
    logger.debug("updateBinaryStream");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateCharacterStream(String s, Reader reader, int i) throws SQLException {
    logger.debug("updateCharacterStream");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateObject(String s, Object o, int i) throws SQLException {
    logger.debug("updateObject");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateObject(String s, Object o) throws SQLException {
    logger.debug("updateObject");
    // TODO Auto-generated method stub
  }

  @Override
  public void insertRow() throws SQLException {
    logger.debug("insertRow");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateRow() throws SQLException {
    logger.debug("updateRow");
    // TODO Auto-generated method stub
  }

  @Override
  public void deleteRow() throws SQLException {
    logger.debug("deleteRow");
    // TODO Auto-generated method stub
  }

  @Override
  public void refreshRow() throws SQLException {
    logger.debug("refreshRow");
    // TODO Auto-generated method stub
  }

  @Override
  public void cancelRowUpdates() throws SQLException {
    logger.debug("cancelRowUpdates");
    // TODO Auto-generated method stub
  }

  @Override
  public void moveToInsertRow() throws SQLException {
    logger.debug("moveToInsertRow");
    // TODO Auto-generated method stub
  }

  @Override
  public void moveToCurrentRow() throws SQLException {
    logger.debug("moveToCurrentRow");
    // TODO Auto-generated method stub
  }

  @Override
  public Statement getStatement() throws SQLException {
    return stat;
  }

  @Override
  public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
    logger.debug("getObject - index/map");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Ref getRef(int i) throws SQLException {
    logger.debug("getRef - index");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Blob getBlob(int i) throws SQLException {
    logger.debug("getBlob - index");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Clob getClob(int i) throws SQLException {
    logger.debug("getClob - index");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Array getArray(int i) throws SQLException {
    logger.debug("getArray - index");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Object getObject(String s, Map<String, Class<?>> map) throws SQLException {
    logger.debug("getObject - name/map");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Ref getRef(String s) throws SQLException {
    logger.debug("getRef - name");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Blob getBlob(String s) throws SQLException {
    logger.debug("getBlob - name");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Clob getClob(String s) throws SQLException {
    logger.debug("getClob - name");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Array getArray(String s) throws SQLException {
    logger.debug("getArray - name");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Date getDate(int i, Calendar calendar) throws SQLException {
    logger.debug("getDate - index/cal");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Date getDate(String s, Calendar calendar) throws SQLException {
    logger.debug("getDate - name/cal");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Time getTime(int i, Calendar calendar) throws SQLException {
    logger.debug("getTime - index/cal");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Time getTime(String s, Calendar calendar) throws SQLException {
    logger.debug("getTime - name/cal");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Timestamp getTimestamp(int i, Calendar calendar) throws SQLException {
    logger.debug("getTimestamp - index/cal");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Timestamp getTimestamp(String s, Calendar calendar) throws SQLException {
    logger.debug("getTimestamp - name/cal");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public URL getURL(int i) throws SQLException {
    logger.debug("getURL - index");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public URL getURL(String s) throws SQLException {
    logger.debug("getURL - name");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void updateRef(int i, Ref ref) throws SQLException {
    logger.debug("updateRef");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateRef(String s, Ref ref) throws SQLException {
    logger.debug("updateRef");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateBlob(int i, Blob blob) throws SQLException {
    logger.debug("updateBlob");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateBlob(String s, Blob blob) throws SQLException {
    logger.debug("updateBlob");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateClob(int i, Clob clob) throws SQLException {
    logger.debug("updateClob");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateClob(String s, Clob clob) throws SQLException {
    logger.debug("updateClob");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateArray(int i, Array array) throws SQLException {
    logger.debug("updateArray");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateArray(String s, Array array) throws SQLException {
    logger.debug("updateArray");
    // TODO Auto-generated method stub
  }

  @Override
  public RowId getRowId(int i) throws SQLException {
    logger.debug("getRowId - index");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public RowId getRowId(String s) throws SQLException {
    logger.debug("getRowId - name");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void updateRowId(int i, RowId rowId) throws SQLException {
    logger.debug("updateRowId");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateRowId(String s, RowId rowId) throws SQLException {
    logger.debug("updateRowId");
    // TODO Auto-generated method stub
  }

  @Override
  public int getHoldability() throws SQLException {
    logger.debug("getHoldability");
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean isClosed() throws SQLException {
    logger.debug("isClosed");
    return cursor == null;
  }

  @Override
  public void updateNString(int i, String s) throws SQLException {
    logger.debug("updateNString");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateNString(String s, String s1) throws SQLException {
    logger.debug("updateNString");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateNClob(int i, NClob nClob) throws SQLException {
    logger.debug("updateNClob");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateNClob(String s, NClob nClob) throws SQLException {
    logger.debug("updateNClob");
    // TODO Auto-generated method stub
  }

  @Override
  public NClob getNClob(int i) throws SQLException {
    logger.debug("getNClob - index");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public NClob getNClob(String s) throws SQLException {
    logger.debug("getNClob - name");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SQLXML getSQLXML(int i) throws SQLException {
    logger.debug("getSQLXML - index");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public SQLXML getSQLXML(String s) throws SQLException {
    logger.debug("getSQLXML - name");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void updateSQLXML(int i, SQLXML sqlxml) throws SQLException {
    logger.debug("updateSQLXML");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateSQLXML(String s, SQLXML sqlxml) throws SQLException {
    logger.debug("updateSQLXML");
    // TODO Auto-generated method stub
  }

  @Override
  public String getNString(int i) throws SQLException {
    logger.debug("getNString - index");
    return getString(i);
  }

  @Override
  public String getNString(String s) throws SQLException {
    logger.debug("getNString - name");
    return getString(s);
  }

  @Override
  public Reader getNCharacterStream(int i) throws SQLException {
    logger.debug("getNCharacterStream - index");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Reader getNCharacterStream(String s) throws SQLException {
    logger.debug("getNCharacterStream - name");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void updateNCharacterStream(int i, Reader reader, long l) throws SQLException {
    logger.debug("updateNCharacterStream");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateNCharacterStream(String s, Reader reader, long l) throws SQLException {
    logger.debug("updateNCharacterStream");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateAsciiStream(int i, InputStream inputStream, long l) throws SQLException {
    logger.debug("updateAsciiStream");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateBinaryStream(int i, InputStream inputStream, long l) throws SQLException {
    logger.debug("updateBinaryStream");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateCharacterStream(int i, Reader reader, long l) throws SQLException {
    logger.debug("updateCharacterStream");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateAsciiStream(String s, InputStream inputStream, long l) throws SQLException {
    logger.debug("updateAsciiStream");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateBinaryStream(String s, InputStream inputStream, long l) throws SQLException {
    logger.debug("updateBinaryStream");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateCharacterStream(String s, Reader reader, long l) throws SQLException {
    logger.debug("updateCharacterStream");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateBlob(int i, InputStream inputStream, long l) throws SQLException {
    logger.debug("updateBlob");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateBlob(String s, InputStream inputStream, long l) throws SQLException {
    logger.debug("updateBlob");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateClob(int i, Reader reader, long l) throws SQLException {
    logger.debug("updateClob");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateClob(String s, Reader reader, long l) throws SQLException {
    logger.debug("updateClob");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateNClob(int i, Reader reader, long l) throws SQLException {
    logger.debug("updateNClob");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateNClob(String s, Reader reader, long l) throws SQLException {
    logger.debug("updateNClob");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateNCharacterStream(int i, Reader reader) throws SQLException {
    logger.debug("updateNCharacterStream");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateNCharacterStream(String s, Reader reader) throws SQLException {
    logger.debug("updateNCharacterStream");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateAsciiStream(int i, InputStream inputStream) throws SQLException {
    logger.debug("updateAsciiStream");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateBinaryStream(int i, InputStream inputStream) throws SQLException {
    logger.debug("updateBinaryStream");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateCharacterStream(int i, Reader reader) throws SQLException {
    logger.debug("updateCharacterStream");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateAsciiStream(String s, InputStream inputStream) throws SQLException {
    logger.debug("updateAsciiStream");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateBinaryStream(String s, InputStream inputStream) throws SQLException {
    logger.debug("updateBinaryStream");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateCharacterStream(String s, Reader reader) throws SQLException {
    logger.debug("updateCharacterStream");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateBlob(int i, InputStream inputStream) throws SQLException {
    logger.debug("updateBlob");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateBlob(String s, InputStream inputStream) throws SQLException {
    logger.debug("updateBlob");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateClob(int i, Reader reader) throws SQLException {
    logger.debug("updateClob");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateClob(String s, Reader reader) throws SQLException {
    logger.debug("updateClob");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateNClob(int i, Reader reader) throws SQLException {
    logger.debug("updateNClob");
    // TODO Auto-generated method stub
  }

  @Override
  public void updateNClob(String s, Reader reader) throws SQLException {
    logger.debug("updateNClob");
    // TODO Auto-generated method stub
  }

  @Override
  public <T> T getObject(int i, Class<T> aClass) throws SQLException {
    logger.debug("getObject - name/class");
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> T getObject(String s, Class<T> aClass) throws SQLException {
    logger.debug("getObject - name/class");
    // TODO Auto-generated method stub
    return null;
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


  protected Object getFieldValue(String selectExpression) {
    logger.debug("getFieldValue " + selectExpression);
    StringTokenizer tokenizer = new StringTokenizer(selectExpression, PROPERTY_SEPARATOR);
    Object tempNode = null;
//    if (curNode != null)
//      tempNode = curNode;
    boolean first = true;
    while (tokenizer.hasMoreTokens()) {
      String currentToken = tokenizer.nextToken();
      int currentTokenLength = currentToken.length();
      int indexOfLeftSquareBracket = currentToken.indexOf(ARRAY_LEFT);

      // got Left Square Bracket - LSB
      if (indexOfLeftSquareBracket != -1) {
        // a Right Square Bracket must be the last character in the current token
        if (currentToken.lastIndexOf(ARRAY_RIGHT) != (currentTokenLength - 1)) {
          return null;
        }

        // LSB not first character
        if (indexOfLeftSquareBracket > 0) {
          // extract nodes at property
          String property = currentToken.substring(0, indexOfLeftSquareBracket);
          tempNode = getTempNode(tempNode, property);
        }

        String arrayOperators = currentToken.substring(indexOfLeftSquareBracket);
        StringTokenizer arrayOpsTokenizer = new StringTokenizer(arrayOperators, ARRAY_RIGHT);
        while (arrayOpsTokenizer.hasMoreTokens()) {
          if (!(tempNode instanceof List)) {
            return null;
          }

          String currentArrayOperator = arrayOpsTokenizer.nextToken();
          tempNode = ((List<?>) tempNode).get(Integer.parseInt(currentArrayOperator.substring(1)));
        }
      } else {
        tempNode = getTempNode(tempNode, currentToken);
      }
      if (first && tempNode == null)
        break;
      first = false;
    }
    return tempNode;
  }

  private Object getTempNode(Object tempNode, String currentToken) {
    if (tempNode == null) {
      if (curDoc != null) {
        if ("_key".equals(currentToken))
          tempNode = curDoc.getKey();
        else if ("_id".equals(currentToken))
          tempNode = curDoc.getId();
        else if ("_rev".equals(currentToken))
          tempNode = curDoc.getRevision();
        else
          tempNode = curDoc.getAttribute(currentToken);
      }
    } else {
      if (tempNode instanceof Map)
        tempNode = ((Map<?, ?>) tempNode).get(currentToken);
    }
    return tempNode;
  }

}
