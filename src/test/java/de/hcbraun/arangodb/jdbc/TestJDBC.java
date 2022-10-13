package de.hcbraun.arangodb.jdbc;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.DbName;
import com.arangodb.mapping.ArangoJack;
import com.arangodb.model.CollectionCreateOptions;
import com.arangodb.model.CollectionSchema;
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
    dbBld.serializer(new ArangoJack());
    dbBld.user(user);
    dbBld.password(password);
    ArangoDB db = dbBld.build();
    ArangoDatabase database = db.db(DbName.of(databaseName));
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
      col.importDocuments("[{\"_key\":\"AC\",\"name\":\"Ascension\",\"isoCode2\":\"AC\",\"isoCode3\":\"ASC\",\"telephoneAreaCode\":\"+247\",\"timeZone\":\"-1\",\"language\":\"EN\",\"eu\":false,\"currency\":\"EUR\"}," +
        "{\"_key\":\"BE\",\"name\":\"Belgium\",\"isoCode2\":\"BE\",\"isoCode3\":\"BEL\",\"telephoneAreaCode\":\"+32\",\"timeZone\":\"+1\",\"language\":\"EN\",\"eu\":true,\"addressFormat\":\"100\",\"region\":\"06\",\"currency\":\"EUR\"}," +
        "{\"_key\":\"DE\",\"name\":\"Deutschland\",\"isoCode2\":\"DE\",\"isoCode3\":\"DEU\",\"telephoneAreaCode\":\"+49\",\"timeZone\":\"+1\",\"language\":\"DE\",\"eu\":true,\"addressFormat\":\"100\",\"region\":\"01\",\"currency\":\"EUR\"}]");
    }
    col = database.collection("Region");
    if (!col.exists()) {
      CollectionCreateOptions cco = new CollectionCreateOptions();
      CollectionSchema cs = new CollectionSchema();
      cs.setLevel(CollectionSchema.Level.NONE);
      cs.setRule("{\"properties\":{\"_key\":{\"type\":\"string\"},\"_id\":{\"type\":\"string\"},\"name\":{\"type\":\"string\"}},\"additionalProperties\":true,\"required\":[\"name\"]}");
      cco.schema(cs);
      col.create(cco);
      col.importDocuments("[{\"_key\":\"01\",\"name\":\"Deutschland\"},{\"_key\":\"06\",\"name\":\"Westeuropa\"}]");
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

}
