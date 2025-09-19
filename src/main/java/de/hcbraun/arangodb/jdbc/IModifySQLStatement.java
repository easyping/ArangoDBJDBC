package de.hcbraun.arangodb.jdbc;

public interface IModifySQLStatement {
  public String modifySQLBeforeExecute(String sql);
}
