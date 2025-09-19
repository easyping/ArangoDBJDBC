package de.hcbraun.arangodb.jdbc;

public interface IModifyAQL {
  public void modifyAQLBeforeExecute(QueryInfo qi);
}
