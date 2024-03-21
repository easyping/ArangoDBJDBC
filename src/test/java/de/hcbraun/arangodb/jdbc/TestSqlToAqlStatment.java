package de.hcbraun.arangodb.jdbc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestSqlToAqlStatment {

  @Test
  public void testSimpleSQL() {
    assertEquals("FOR c1 IN Country RETURN c1",
      (new ArangoDBStatement(null)).getAQL("select * from Country", null).aql);
  }

  @Test
  public void testSimpleSQLWithColumns() {
    assertEquals("FOR c1 IN Country RETURN {_key:c1._key,name:c1.name}",
      (new ArangoDBStatement(null)).getAQL("select _key,name from Country", null).aql);
  }

  @Test
  public void testSimpleAQL() {
    assertEquals("FOR c IN Country RETURN c",
      (new ArangoDBStatement(null)).getAQL("FOR c IN Country RETURN c", null).aql);
  }

  @Test
  public void testSimpleSQLWhereFixValue() {
    assertEquals("FOR c1 IN Country FILTER c1.land=='DE' RETURN c1",
      (new ArangoDBStatement(null)).getAQL("select * from Country where Country.land='DE'", null).aql);
    assertEquals("FOR c1 IN Country FILTER c1.land=='AT' RETURN c1",
      (new ArangoDBStatement(null)).getAQL("select * from Country where land='AT'", null).aql);
    assertEquals("FOR c1 IN Country FILTER LIKE(c1.land,'D%') RETURN c1",
      (new ArangoDBStatement(null)).getAQL("select * from Country where land like 'D%'", null).aql);
  }

  @Test
  public void testSimpleSQLWithAs() {
    assertEquals("FOR a IN Country RETURN a",
      (new ArangoDBStatement(null)).getAQL("select * from Country a", null).aql);
  }

  @Test  //Spalten Analyse komplett umdenken, Spalten zusammenfassen (description)
  public void testSimpleSQLWithColumns2() {
    assertEquals("FOR a IN DeliveryTerm RETURN {_key:a._key,name:a.name,description:{DE:a.description.DE,EN:a.description.EN,FR:a.description.FR,NL:a.description.NL}}",
      (new ArangoDBStatement(null)).getAQL("select a._key,a.name,a.description.DE,a.description.EN,a.description.FR,a.description.NL from DeliveryTerm a", null).aql);
  }

  @Test
  public void testJoinSQL() {
    assertEquals("FOR c1 IN Country FOR c2 IN City FILTER c1._key==c2.land RETURN MERGE(c1,c2)",
      (new ArangoDBStatement(null)).getAQL("select * from Country, City where Country._key=City.land", null).aql);
  }

  @Test
  public void testJoinSQLWithColumns() {
    assertEquals("FOR c1 IN Country FOR c2 IN City FILTER c1._key==c2.land RETURN {land:c1._key,plz:c2.plz}",
      (new ArangoDBStatement(null)).getAQL("select Country._key land,City.plz from Country, City where Country._key=City.land", null).aql);
  }

  @Test
  public void testJoinSQLWithColumnsAndSort() {
    assertEquals("FOR c1 IN Country FOR c2 IN City FILTER c1._key==c2.land SORT c1._key RETURN {land:c1._key,plz:c2.plz}",
      (new ArangoDBStatement(null)).getAQL("select Country._key land,City.plz from Country, City where Country._key=City.land order by Country._key", null).aql);
  }

  @Test
  public void testJoinOnSQLWithColumnsAndSort() {
    assertEquals("FOR c1 IN City LET c2=DOCUMENT('Country',c1.land) FILTER c2._key SORT c2._key RETURN {land:c2._key,plz:c1.plz}",
      (new ArangoDBStatement(null)).getAQL("select Country._key land,City.plz from City join Country on Country._key=City.land order by Country._key", null).aql);
  }

  @Test
  public void testOuterJoinOnSQLWithColumnsAndSort() {
    assertEquals("FOR c1 IN City LET c2=DOCUMENT('Country',c1.land) SORT c2._key RETURN {land:c2._key,plz:c1.plz}",
      (new ArangoDBStatement(null)).getAQL("select Country._key land,City.plz from City outer join Country on Country._key=City.land order by Country._key", null).aql);
  }

  @Test
  public void testOuterJoinOnSQL() {
    assertEquals("FOR c1 IN employee LET c2=(FOR c3 IN vehicle FILTER c1.vehicle_id==c3.vId RETURN c3) RETURN MERGE(c1,c2)",
      (new ArangoDBStatement(null)).getAQL("select * from employee outer join vehicle on employee.vehicle_id=vehicle.vId", null).aql);
  }

  @Test
  public void testOuterJoinOnSQL_id() {
    assertEquals("FOR c1 IN employee LET c2=DOCUMENT(c1.vehicle_id) RETURN MERGE(c1,c2)",
      (new ArangoDBStatement(null)).getAQL("select * from employee outer join vehicle on employee.vehicle_id=vehicle._id", null).aql);
  }

  @Test
  public void testComments() {
    assertEquals("FOR c1 IN Country RETURN {land:c1.land,plz:c1.plz}",
      (new ArangoDBStatement(null)).getAQL("select land,plz  /* cols: land:s,plz:s */ from Country // Test", null).aql);
  }

  @Test
  public void testRemoveNewLineTabs() {
    assertEquals("FOR c1 IN Country RETURN {name:c1.name,_key:c1._key,eu:c1.eu,language:c1.language}",
      (new ArangoDBStatement(null)).getAQL("SELECT \tCountry.name\n,\tCountry._key\n\r,\tCountry.eu,\r\n\tCountry.language FROM Country", null).aql);
  }

  @Test
  public void testSubColumns() {
    assertEquals("FOR c1 IN Supplier RETURN {_key:c1._key,supplierText:{DE:c1.supplierText.DE,EN:c1.supplierText.EN,FR:c1.supplierText.FR,NL:c1.supplierText.NL}," +
        "address:{country:c1.address.country,website:c1.address.website,city:c1.address.city,postBox:c1.address.postBox}}",
      (new ArangoDBStatement(null)).getAQL(" SELECT Supplier._key,Supplier.supplierText.DE,Supplier.supplierText.EN,Supplier.supplierText.FR," +
        "Supplier.address.country,Supplier.supplierText.NL,Supplier.address.website,Supplier.address.city,Supplier.address.postBox FROM Supplier", null).aql);
  }

  @Test
  public void testSimpleGroupBy() {
    assertEquals("FOR c1 IN Article FILTER (c1.group>=@p1 && c1.group<=@p2) COLLECT g0=c1.type RETURN {type:g0}",
      (new ArangoDBStatement(null)).getAQL("SELECT type FROM Article WHERE group BETWEEN ? AND ? GROUP BY type", null).aql);
  }

  @Test
  public void testSimpleGroupByHaving() {
    assertEquals("FOR c1 IN Article FILTER (c1.group>=@p1 && c1.group<=@p2) COLLECT g0=c1.type AGGREGATE ag1=COUNT(c1.type) FILTER ag1<5 RETURN {type:g0}",
      (new ArangoDBStatement(null)).getAQL("SELECT type FROM Article WHERE group BETWEEN ? AND ? GROUP BY type HAVING COUNT(type)<5", null).aql);
  }

  @Test
  public void testSQLWithSubSelect() {
    assertEquals("FOR c1 IN Country RETURN {name:c1.name,rName:(FOR c2 IN Region FILTER c2._key==c1.region RETURN c2.name)[0]}",
            (new ArangoDBStatement(null)).getAQL("select name,(select name from Region where Region._key=Country.region) rName from Country", null).aql);
//    assertEquals("FOR c1 IN Country RETURN {name:c1.name,rName:DOCUMENT('Region',c1.region).name}",
//            (new ArangoDBStatement(null)).getAQL("select name,(select name from Region where Region._key=Country.region) rName from Country", null).aql);
  }

  @Test
  public void testSimpleInsert() {
    assertEquals("INSERT {_key:@p1,name:@p2} INTO Country LET inserted = NEW RETURN inserted._key",
      (new ArangoDBStatement(null)).getAQL("INSERT INTO Country (_key,name) VALUES (?,?)", null).aql);
  }

  @Test
  public void testSelectInsert() {
    assertEquals("FOR c1 IN Country FILTER c1._key>@p1 INSERT {name:c1.name,createDate:DATE_ISO8601(DATE_NOW())} INTO Country LET inserted = NEW RETURN inserted._key",
      (new ArangoDBStatement(null)).getAQL("INSERT INTO Country (name,createDate) SELECT name,CURRENT_TIMESTAMP FROM Country WHERE _key>?", null).aql);
  }

  @Test
  public void testSimpleUpdate() {
    assertEquals("FOR c1 IN Country FILTER c1._key==@p2 UPDATE c1._key WITH {name:@p1} IN Country LET updated = NEW RETURN updated._key",
      (new ArangoDBStatement(null)).getAQL("UPDATE Country SET name=? WHERE _key=?", null).aql);
  }

  @Test
  public void testSimpleDelete() {
    assertEquals("FOR c1 IN Country FILTER c1._key==@p1 REMOVE c1._key IN Country LET removed = OLD RETURN removed._key",
      (new ArangoDBStatement(null)).getAQL("DELETE Country WHERE _key=?", null).aql);
  }

  @Test
  public void testSimpleWithIn() {
    assertEquals("FOR c1 IN Article FILTER c1.group IN ['AB','CD','EF'] RETURN {type:c1.type}",
      (new ArangoDBStatement(null)).getAQL("SELECT type FROM Article WHERE group IN ('AB', 'CD', 'EF')", null).aql);
  }

  @Test
  public void testSimpleWithInSelect() {
    assertEquals("FOR c1 IN Article FILTER c1.group IN ((FOR c2 IN AGroup RETURN c2.group)) RETURN {type:c1.type}",
      (new ArangoDBStatement(null)).getAQL("SELECT type FROM Article WHERE group IN (SELECT group FROM AGroup)", null).aql);
  }

}
