package de.hcbraun.arangodb.jdbc;

import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.Protocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class ArangoDBConnection implements Connection {

  private final Logger logger = LoggerFactory.getLogger(ArangoDBConnection.class);

  private ArangoDatabase database = null;
  private String userName = null;
  private String schema = "adbdbo";

  protected ArangoDBConnection(String host, String port, HashMap<String, String> lstPara) {
    String[] pdb = port.split("/");
    ArangoDB.Builder dbBld = new ArangoDB.Builder().host(host, Integer.parseInt(pdb[0]));

    for (String key : lstPara.keySet()) {
      if (!"password".equals(key))
        System.out.println(key + ": " + lstPara.get(key));
      if ("user".equals(key))
        dbBld = dbBld.user(userName = lstPara.get(key));
      else if ("password".equals(key))
        dbBld = dbBld.password(lstPara.get(key));
      else if ("timeout".equals(key))
        dbBld = dbBld.timeout(Integer.parseInt(lstPara.get(key)));
      else if ("useSsl".equals(key))
        dbBld = dbBld.useSsl("true".equalsIgnoreCase(lstPara.get(key)));
      else if ("chunksize".equals(key))
        dbBld = dbBld.chunkSize(Integer.parseInt(lstPara.get(key)));
      else if ("connections.max".equals(key))
        dbBld = dbBld.maxConnections(Integer.parseInt(lstPara.get(key)));
      else if ("protocol".equals(key))
        dbBld = dbBld.protocol("HTTP-JSON".equalsIgnoreCase(lstPara.get(key)) ? Protocol.HTTP_JSON : "HTTP-VPACK".equalsIgnoreCase(lstPara.get(key)) ? Protocol.HTTP_VPACK : Protocol.VST);
    }
    String databaseName = pdb.length > 1 ? pdb[1] : lstPara.get("database");

    ArangoDB db = dbBld.build();
    database = db.db(databaseName);
    schema = databaseName;
//    System.out.println("Database-Version: " + database.getVersion().getVersion());
  }

  protected String getUserName() {
    return userName;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - unwarp");
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - isWrapperFor");
    return false;
  }

  @Override
  public Statement createStatement() throws SQLException {
    logger.debug("createStatement");
    return new ArangoDBStatement(this);
  }

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    logger.debug("prepareStatement " + sql);
    return new ArangoDBPreparedStatement(this, sql);
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - prepareCall");
    return null;
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - nativeSQL");
    return null;
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - setAutoCommit");
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - getAutoCommit");
    return true;
  }

  @Override
  public void commit() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - commit");
  }

  @Override
  public void rollback() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - rollback");
  }

  @Override
  public void close() throws SQLException {
    database = null;
    logger.debug("Connection - close");
  }

  @Override
  public boolean isClosed() throws SQLException {
    logger.debug("Connection - isClose");
    return database == null;
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    logger.debug("Connection - getMetaData");
    return new ArangoDBMetaData(this, schema);
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - setReadOnly");
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - isReadOnly");
    return false;
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - setCatalog: " + catalog);
  }

  @Override
  public String getCatalog() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - getCatalog");
    return null;
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - setTransactionIsolation");
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - getTransactionIsolation");
    return TRANSACTION_NONE;
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - getWarnings");
    return null;
  }

  @Override
  public void clearWarnings() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - clearWarnings");
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - createStatement-1");
    return new ArangoDBStatement(this);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - prepareStatement-1");
    return new ArangoDBPreparedStatement(this, sql);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - prepareCall-1");
    return null;
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - getTypeMap");
    return null;
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - setTypeMap");
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - setHoldability");
  }

  @Override
  public int getHoldability() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - getHoldability");
    return 0;
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - setSavepoint");
    return null;
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - setSavepoint-1");
    return null;
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - rollback-1");
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - releaseSavepoint");
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - createStatement-2");
    return new ArangoDBStatement(this);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - prepareStatement-2");
    return new ArangoDBPreparedStatement(this, sql);
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - prepareCall-2");
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - prepareStatement-3");
    return new ArangoDBPreparedStatement(this, sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - prepareStatement-4");
    return new ArangoDBPreparedStatement(this, sql);
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - prepareStatement-5");
    return new ArangoDBPreparedStatement(this, sql);
  }

  @Override
  public Clob createClob() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - createClob");
    return null;
  }

  @Override
  public Blob createBlob() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - createBlob");
    return null;
  }

  @Override
  public NClob createNClob() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - createNClob");
    return null;
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - createSQLXML");
    return null;
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - isValid");
    return true;
  }

  @Override
  public void setClientInfo(String name, String value) throws SQLClientInfoException {
    // TODO Auto-generated method stub
    logger.debug("Connection - setClientInfo");
  }

  @Override
  public void setClientInfo(Properties properties) throws SQLClientInfoException {
    // TODO Auto-generated method stub
    logger.debug("Connection - setClientInfo-1");
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - getClientInfo");
    return null;
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - getClientInfo-1");
    return null;
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - createArrayOf");
    return null;
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - createStuct");
    return null;
  }

  @Override
  public void setSchema(String schema) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - setSchema");
  }

  @Override
  public String getSchema() throws SQLException {
    logger.debug("Connection - getSchema: " + (schema != null ? schema : null));
    return schema;
  }

  @Override
  public void abort(Executor executor) throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - abort");
  }

  @Override
  public void setNetworkTimeout(Executor executor, int milliseconds)
          throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - setNetworkTimeout");
  }

  @Override
  public int getNetworkTimeout() throws SQLException {
    // TODO Auto-generated method stub
    logger.debug("Connection - getNetworkTimeout");
    return 0;
  }

  protected ArangoDatabase getDatabase() {
    return database;
  }
}
