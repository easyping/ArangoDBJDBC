package de.hcbraun.arangodb.jdbc;

import java.sql.*;
import java.util.HashMap;
import java.util.Properties;
import java.util.logging.Logger;

public class ArangoDBDriver implements Driver {
  public final static String URL_PREFIX = "jdbc:hcbraun:arangodb";

  public Connection connect(String url, Properties info) throws SQLException {
    // Beispiel:  jdbc:hcbraun:arangodb:localhost:8529/testdb

    String[] part = url.split("/");
    String[] hostInfo = part[0].split(":");
    String host = "127.0.0.1", port = "8529";
    if (hostInfo.length > 3)
      host = hostInfo[3];
    if (hostInfo.length > 4)
      port = hostInfo[4];

    String[] urlPart = part[1].split(";");
    HashMap<String, String> lstPara = new HashMap<>();
    lstPara.put("database", urlPart[0]);
    for (int i = 1; i < urlPart.length; i++) {
      String[] para = urlPart[i].split("=");
      if (para.length > 1)
        lstPara.put(para[0], para[1]);
    }
    if (lstPara.get("user") == null)
      lstPara.put("user", (String) info.get("user"));
    if (lstPara.get("password") == null)
      lstPara.put("password", (String) info.get("password"));

    return new ArangoDBConnection(host, port, lstPara);
  }

  @Override
  public boolean acceptsURL(String url) throws SQLException {
    return url.startsWith(URL_PREFIX) && url.indexOf("/") > 0;
  }

  @Override
  public DriverPropertyInfo[] getPropertyInfo(String url, Properties info)
          throws SQLException {
    return new DriverPropertyInfo[0];
  }

  @Override
  public int getMajorVersion() {
    return ArangoDBJDBCVersion.major;
  }

  @Override
  public int getMinorVersion() {
    return ArangoDBJDBCVersion.minor;
  }

  @Override
  public boolean jdbcCompliant() {
    return false;
  }

  @Override
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException("methodNotSupported: Driver.getParentLogger()");
  }

  static {
    try {
      java.sql.DriverManager.registerDriver(new ArangoDBDriver());
    } catch (SQLException e) {
      throw new RuntimeException("ArangoDBDriver-Init-Error: " + e.getMessage());
    }
  }

}
