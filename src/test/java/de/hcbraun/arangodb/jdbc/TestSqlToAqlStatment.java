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

  @Test
  public void testSimpleSelectWithSeparator() {
    assertEquals("FOR c1 IN Article FILTER c1.changeInfo.createUser=='9304' RETURN {type:c1.type}",
      (new ArangoDBStatement(null, "__")).getAQL("SELECT type FROM Article WHERE changeInfo__createUser='9304'", null).aql);
  }

  @Test
  public void testSimpleSelectName() {
    // Since no connection is specified, make the schema substitution in the test.
    assertEquals("FOR c1 IN BusinessPartner COLLECT g0=c1.name RETURN {BusinessPartner_name:g0}",
      (new ArangoDBStatement(null)).getAQL("select BusinessPartner.name as BusinessPartner_name from TESTDB.BusinessPartner BusinessPartner group by BusinessPartner.name".replaceAll("(( |,)TESTDB\\.)" , " "), null).aql);
  }

  @Test
  public void testSimpleSelectOrderState() {
    // Since no connection is specified, make the schema substitution in the test.
    assertEquals("FOR c1 IN Order COLLECT g0=c1.state SORT g0 RETURN {state:g0}",
      (new ArangoDBStatement(null)).getAQL("select state\n" +
        "from TESTDB.Order\n" +
        "group by state\n" +
        "order by state".replaceAll("(( |,)TESTDB\\.)" , " "), null).aql);
  }

  @Test
  public void testSimpleSelectOrderCustomerSum() {
    assertEquals("FOR c1 IN Order1 COLLECT g0=c1.customerBP AGGREGATE ag1=Sum(c1.priceTotal) SORT g0 RETURN {Order_customerBP:g0,Sum_Order_priceTotal:ag1}",
      (new ArangoDBStatement(null)).getAQL("select Order1.customerBP as Order_customerBP, \n" +
        "Sum(Order1.priceTotal) as Sum_Order_priceTotal\n" +
        "from Order1 Order1\n" +
        "group by Order1.customerBP\n" +
        "order by Order_customerBP", null).aql);
  }

  @Test
  public void testSimpleSelectOrderSumPriceGroupByNameAndBP() {
    assertEquals("FOR c1 IN aOrder LET c2=DOCUMENT('Address',c1.customerAddress) FILTER c1.state=='40' COLLECT g0=c1.customerBP,g1=c2.name1 AGGREGATE ag1=Sum(c1.priceTotal) SORT g0,g1 RETURN {aOrder_customerBP:g0,Address_name1:g1,Sum_aOrder_priceTotal:ag1}",
      (new ArangoDBStatement(null)).getAQL("select Address.name1 as Address_name1, \n" +
        "Sum(aOrder.priceTotal) as Sum_aOrder_priceTotal, \n" +
        "aOrder.customerBP as aOrder_customerBP\n" +
        "from aOrder aOrder\n" +
        "inner join Address Address on (aOrder.customerAddress = Address._key)\n" +
        "where (aOrder.state = '40')\n" +
        "group by aOrder.customerBP, Address.name1\n" +
        "order by aOrder_customerBP, Address_name1", null).aql);
  }

  @Test
  public void testSimpleSelectOrderMinMaxDate() {
    assertEquals("FOR c1 IN aOrder FILTER c1.state=='40' COLLECT AGGREGATE ag1=Max(c1.orderDate),ag2=Min(c1.orderDate) RETURN {Max_aOrder_orderDate:ag1,Min_aOrder_orderDate:ag2}",
      (new ArangoDBStatement(null)).getAQL("select Max(aOrder.orderDate) as Max_aOrder_orderDate, \n" +
        "Min(aOrder.orderDate) as Min_aOrder_orderDate\n" +
        "from TESTHNLERP.aOrder aOrder\n" +
        "where (aOrder.state = '40')", null).aql);
  }

  @Test
  public void testSimpleSelectWithFunction() {
    assertEquals("FOR c1 IN aOrder RETURN {lowerDesc:LOWER(c1.description),lenOfDesc:LENGTH(c1.description)}",
      (new ArangoDBStatement(null)).getAQL("select Lower(aOrder.description) as lowerDesc, len(aOrder.description) as lenOfDesc \n" +
        "from aOrder aOrder", null).aql);
  }

  @Test
  public void testSimpleSelectWithTimestampFunction() {
    assertEquals("FOR c1 IN aOrder LET c2=DOCUMENT('Address',c1.customerAddress) FILTER (c1.orderDate>='2022-04-30T22:00:00.000Z' && c1.orderDate<='2022-05-01T21:59:59.999Z') && c1.state=='40' COLLECT g0=c1.customerBP,g1=c2.name1 AGGREGATE ag1=Sum(c1.priceTotal) SORT g0,g1 RETURN {aOrder_customerBP:g0,Address_name1:g1,Sum_aOrder_priceTotal:ag1}",
      (new ArangoDBStatement(null)).getAQL("select Address.name1 as Address_name1, Sum(aOrder.priceTotal) as Sum_aOrder_priceTotal, aOrder.customerBP as aOrder_customerBP " +
        "from aOrder aOrder inner join Address Address on (aOrder.customerAddress = Address._key) " +
        "where ((aOrder.orderDate between timestamp('2022-05-01 00:00:00') and timestamp('2022-05-01 00:00:00')) and aOrder.state = '40') " +
        "group by aOrder.customerBP, Address.name1 order by aOrder_customerBP, Address_name1", null).aql);
  }

}
