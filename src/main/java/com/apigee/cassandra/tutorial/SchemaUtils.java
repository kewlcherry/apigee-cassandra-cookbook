package com.apigee.cassandra.tutorial;

import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;

/**
 * Common utility class for checking schema existence and
 * creating if necessary.
 *
 * @author zznate
 */
public class SchemaUtils {

  public static String COOKBOOK_KEYSPACE_NAME = "CookbookKeyspace";

  private Cluster cluster;

  public SchemaUtils(Cluster cluster) {
    this.cluster = cluster;
  }

  public boolean cfExists(String columnFamilyNme) {
    KeyspaceDefinition ksDef = cluster.describeKeyspace(COOKBOOK_KEYSPACE_NAME);
    if ( ksDef == null ) {
    	maybeCreateKeyspace();
    }
    for ( ColumnFamilyDefinition cfDef : ksDef.getCfDefs() ) {
      if (cfDef.getName().equals(columnFamilyNme)) {
        return true;
      }
    }
    return false;
  }

  public void maybeCreate(ColumnFamilyDefinition cfDef) {
    if ( cfExists(cfDef.getName() ) ) {
      return;
    }
    cluster.addColumnFamily(cfDef);
  }

  public void maybeCreateKeyspace() {
    if ( cluster.describeKeyspace(COOKBOOK_KEYSPACE_NAME) == null ) {
      cluster.addKeyspace(new ThriftKsDef(COOKBOOK_KEYSPACE_NAME));
    }
  }

  /**
   * Mark all existing data in this column family as deleted. Functions
   * similarly to an RDBMS.
   *
   * @param columnFamilyName
   */
  public void truncate(String columnFamilyName) {
    cluster.truncate(COOKBOOK_KEYSPACE_NAME, columnFamilyName);
  }
}