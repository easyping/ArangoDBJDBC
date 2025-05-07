package de.hcbraun.arangodb.jdbc;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.model.CollectionCreateOptions;
import com.arangodb.model.CollectionSchema;
import com.arangodb.util.RawJson;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.*;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestJDBC {

  private static String host = "localhost:8529", databaseName = "TestJDBC", user = "testJdbc", password = "pwd4Test&jdbc";

  private Connection getConnection() throws ClassNotFoundException, SQLException {
    Class.forName("de.hcbraun.arangodb.jdbc.ArangoDBDriver");
    return DriverManager.getConnection("jdbc:hcbraun:arangodb:" + host + "/" + databaseName, user, password);
  }

  @BeforeAll
  public static void initDatabase() {
    String[] h = host.split(":");
    ArangoDB.Builder dbBld = new ArangoDB.Builder().host(h[0], Integer.parseInt(h[1]));
    dbBld.user(user);
    dbBld.password(password);
    ArangoDB db = dbBld.build();
    ArangoDatabase database = db.db(databaseName);
    ArangoCollection col = database.collection("Country");
    if (!col.exists()) {
      CollectionCreateOptions cco = new CollectionCreateOptions();
      CollectionSchema cs = new CollectionSchema();
      cs.setLevel(CollectionSchema.Level.NONE);
      cs.setRule("{\"properties\": {\"_key\": {\"type\": \"string\"},\"_id\": {\"type\": \"string\"},\"name\": {\"type\": \"string\"}," +
        "\"isoCode2\": {\"type\": \"string\"},\"isoCode3\": {\"type\": \"string\"},\"telephoneAreaCode\": {\"type\": \"string\"}," +
        "\"timeZone\": {\"type\": \"string\"},\"language\": {\"type\": \"string\"},\"eu\": {\"type\": \"boolean\"}," +
        "\"addressFormat\": {\"type\": \"string\"},\"region\": {\"type\": \"string\"},\"currency\": {\"type\": \"string\"}," +
        "\"invoiceWithWeight\": {\"type\": \"boolean\"},\"salesArea4Region\": {\"type\": \"array\",\"items\": {\"type\": \"string\"}}," +
        "\"region4SalesArea\": {\"type\": \"string\"}},\"additionalProperties\": true,\"required\": [\"name\",\"isoCode2\"," +
        "\"isoCode3\",\"telephoneAreaCode\",\"language\"]}");
      cco.schema(cs);
      col.create(cco);
      col.importDocuments(RawJson.of("[{\"_key\":\"AC\",\"name\":\"Ascension\",\"isoCode2\":\"AC\",\"isoCode3\":\"ASC\",\"telephoneAreaCode\":\"+247\",\"timeZone\":\"-1\",\"language\":\"EN\",\"eu\":false,\"currency\":\"EUR\"}," +
        "{\"_key\":\"BE\",\"name\":\"Belgium\",\"isoCode2\":\"BE\",\"isoCode3\":\"BEL\",\"telephoneAreaCode\":\"+32\",\"timeZone\":\"+1\",\"language\":\"EN\",\"eu\":true,\"addressFormat\":\"100\",\"region\":\"06\",\"currency\":\"EUR\"}," +
        "{\"_key\":\"DE\",\"name\":\"Deutschland\",\"isoCode2\":\"DE\",\"isoCode3\":\"DEU\",\"telephoneAreaCode\":\"+49\",\"timeZone\":\"+1\",\"language\":\"DE\",\"eu\":true,\"addressFormat\":\"100\",\"region\":\"01\",\"currency\":\"EUR\"}]"));
    } else {
      if (col.documentExists("XINS"))
        col.deleteDocument("XINS");
    }
    col = database.collection("Region");
    if (!col.exists()) {
      CollectionCreateOptions cco = new CollectionCreateOptions();
      CollectionSchema cs = new CollectionSchema();
      cs.setLevel(CollectionSchema.Level.NONE);
      cs.setRule("{\"properties\":{\"_key\":{\"type\":\"string\"},\"_id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"additionalProperties\":true,\"required\":[\"name\"]}");
      cco.schema(cs);
      col.create(cco);
      col.importDocuments(RawJson.of("[{\"_key\":\"01\",\"name\":\"Deutschland\"},{\"_key\":\"06\",\"name\":\"Westeuropa\"}]"));
    } else {
      if (col.documentExists("XINS"))
        col.deleteDocument("XINS");
    }
  }

  @Test
  public void testSQLQueryResult() {
    try {
      Connection con = getConnection();
      Statement stat = con.createStatement();
      ResultSet rs = stat.executeQuery("SELECT name,_key,region,eu FROM Country ORDER BY _key");
      if (rs != null) {
        if (rs.next()) {
          assertEquals("_key", rs.getMetaData().getColumnName(2));
          assertEquals("name", rs.getMetaData().getColumnName(1));
          assertEquals("region", rs.getMetaData().getColumnName(3));
          assertEquals(Types.BOOLEAN, rs.getMetaData().getColumnType(4));
          assertEquals("AC", rs.getString("_key"));
        }
        rs.close();
      }
      con.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testAQLQueryResult() {
    try {
      Connection con = getConnection();
      Statement stat = con.createStatement();
      ResultSet rs = stat.executeQuery("FOR c IN Country LET r=DOCUMENT('Region', c.region) SORT c._key RETURN {cName:c.name,_key:c._key,region:c.region,eu:c.eu}");
      if (rs != null) {
        if (rs.next()) {
          assertEquals("_key", rs.getMetaData().getColumnName(2));
          assertEquals("cName", rs.getMetaData().getColumnName(1));
          assertEquals("region", rs.getMetaData().getColumnName(3));
          assertEquals(Types.BOOLEAN, rs.getMetaData().getColumnType(4));
          assertEquals("AC", rs.getString("_key"));
        }
        rs.close();
      }
      con.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testInsertAndDelete() {
    try {
      Connection con = getConnection();
      PreparedStatement pIns = con.prepareStatement("INSERT INTO Country (_key, name, isoCode2, isoCode3, telephoneAreaCode, language, eu) VALUES (?,?,?,?,?,?,?)");
      pIns.setString(1, "XINS");
      pIns.setString(2, "Insert Land");
      pIns.setString(3, "IL");
      pIns.setString(4, "ILD");
      pIns.setString(5, "+99");
      pIns.setString(6, "XX");
      pIns.setBoolean(7, false);
      assertEquals(pIns.executeUpdate(), 1);
      pIns.close();
      PreparedStatement stat = con.prepareStatement("SELECT name,language,eu FROM Country WHERE _key=?");
      stat.setString(1, "XINS");
      ResultSet rs = stat.executeQuery();
      while (rs.next()) {
        assertEquals("Insert Land", rs.getString(1));
        assertEquals(false, rs.getBoolean(3));
        assertEquals("XX", rs.getString(2));
      }
      PreparedStatement pDel = con.prepareStatement("DELETE FROM Country WHERE _key=?");
      pDel.setString(1, "XINS");
      assertEquals(pDel.executeUpdate(), 1);
      pDel.close();
      rs = stat.executeQuery();
      assertEquals(false, rs.next());
      stat.close();
      con.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testUpdate() {
    try {
      Connection con = getConnection();
      PreparedStatement pIns = con.prepareStatement("INSERT INTO Region (_key, name) VALUES ('XINS',?)");
      pIns.setString(1, "Test-Region");
      assertEquals(pIns.executeUpdate(), 1);
      pIns.close();
      PreparedStatement pUpd = con.prepareStatement("UPDATE Region SET name=? WHERE _key=?");
      pUpd.setString(1, "Update-Region");
      pUpd.setString(2, "XINS");
      assertEquals(pUpd.executeUpdate(), 1);
      pUpd.close();
      PreparedStatement stat = con.prepareStatement("SELECT name FROM Region WHERE _key=?");
      stat.setString(1, "XINS");
      ResultSet rs = stat.executeQuery();
      while (rs.next()) {
        assertEquals("Update-Region", rs.getString(1));
      }
      PreparedStatement pDel = con.prepareStatement("DELETE FROM Region WHERE _key=?");
      pDel.setString(1, "XINS");
      assertEquals(pDel.executeUpdate(), 1);
      pDel.close();
      stat.close();
      con.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testMetaData() {
    try {
      Connection con = getConnection();
      DatabaseMetaData dmd = con.getMetaData();
      ResultSet rs = dmd.getTables(null, null, null, null);
      rs.next();
      assertEquals("Country", rs.getString("TABLE_NAME"));
      rs.close();
      rs = dmd.getColumns(null, null, "Country", null);
      rs.next();
      assertEquals("_key", rs.getString("COLUMN_NAME"));
      rs.close();
      rs = dmd.getColumns(null, null, "Country", "eu");
      rs.next();
      assertEquals("eu", rs.getString("COLUMN_NAME"));
      rs.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}
