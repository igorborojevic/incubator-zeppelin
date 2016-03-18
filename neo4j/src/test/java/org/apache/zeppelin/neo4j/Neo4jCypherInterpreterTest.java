/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.zeppelin.neo4j;

import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.util.Pair;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import junit.framework.TestCase;
import static org.junit.Assert.*;
import org.junit.Test;


public class Neo4jCypherInterpreterTest extends TestCase {

  @Test
  public void test1() {


    Driver driver = GraphDatabase.driver("bolt://localhost");
    Session session = driver.session();
    StatementResult result = session.run( "MATCH (tom {name: \"Tom Hanks\"}) RETURN tom" );

    while ( result.hasNext() ) {
      Record record = result.next();
      for ( Pair<String, Value> fieldInRecord : record.fields() ) {
        System.out.println( fieldInRecord.key() + " = " + fieldInRecord.value() );
      }
    }

    session.close();
    try {
      driver.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    assertEquals("ok", "ok");
  }


}
