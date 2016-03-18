/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.zeppelin.neo4j;

import org.apache.zeppelin.interpreter.Interpreter;
import org.apache.zeppelin.interpreter.InterpreterContext;
import org.apache.zeppelin.interpreter.InterpreterPropertyBuilder;
import org.apache.zeppelin.interpreter.InterpreterResult;
import org.apache.zeppelin.interpreter.InterpreterResult.Code;
import org.apache.zeppelin.scheduler.Scheduler;
import org.apache.zeppelin.scheduler.SchedulerFactory;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.summary.Notification;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.util.Pair;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.Neo4jException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


/**
 * Cypher interpreter for Neo4j.
 */
public class Neo4jCypherInterpreter extends Interpreter {

  private Logger logger = LoggerFactory.getLogger(Neo4jCypherInterpreter.class);

  static final String DEFAULT_NEO4J_URL = "bolt://localhost";
  static final String DEFAULT_NEO4J_USER_NAME = "neo4j";
  static final String DEFAULT_NEO4J_USER_PASSWORD = "neo4j";

  static final String NEO4J_SERVER_URL = "neo4j.url";
  static final String NEO4J_SERVER_USER = "neo4j.user";
  static final String NEO4J_SERVER_PASSWORD = "neo4j.password";

  static {
    Interpreter.register("cypher", "cypher", Neo4jCypherInterpreter.class.getName(),
        new InterpreterPropertyBuilder()
            .add(NEO4J_SERVER_URL, DEFAULT_NEO4J_URL, "The URL for Neo4j")
            .add(NEO4J_SERVER_USER, DEFAULT_NEO4J_USER_NAME, "The Neo4j user name")
            .add(NEO4J_SERVER_PASSWORD, DEFAULT_NEO4J_USER_PASSWORD, "The Neo4j user password")
            .build());
  }

  private Session session;
  private Driver driver;
  private Exception exceptionOnConnect;
  private static final List<String> NO_COMPLETION = new ArrayList<String>();

  public Neo4jCypherInterpreter(Properties property) {
    super(property);
  }

  @Override public void open() {

    logger.info("Open Cypher connection!");

    try {
      String url = getProperty(NEO4J_SERVER_URL);
      String user = getProperty(NEO4J_SERVER_USER);
      String password = getProperty(NEO4J_SERVER_PASSWORD);

      driver = GraphDatabase.driver(url);
      session = driver.session();

      logger.info("Successfully created Cypher connection");

    } catch (Neo4jException e) {
      logger.error("Cannot open connection", e);
      exceptionOnConnect = e;
      close();
    }
  }

  @Override public void close() {
    logger.info("Close Cypher connection!");

    try {
      if (session != null) {
        session.close();
        driver.close();
      }
    } catch (Exception e) {
      logger.error("Cannot close connection", e);
    } finally {
      exceptionOnConnect = null;
    }
  }

  @Override public InterpreterResult interpret(String cmd, InterpreterContext contextInterpreter) {
    logger.info("Run Cypher command '{}'", cmd);

    try {
      if (exceptionOnConnect != null) {
        return new InterpreterResult(Code.ERROR, exceptionOnConnect.getMessage());
      }

      StringBuilder msg = new StringBuilder();

      StatementResult result = session.run(cmd);

      /*
      ResultSummary summary = result.summarize();
      msg.append( summary.statementType() + "\n");
      msg.append( summary.profile() + "\n" );

      for ( Notification notification : summary.notifications() ) {
        msg.append( "Notification: " + notification + "\n");
      }
      */
      while ( result.hasNext() ) {
        Record record = result.next();
        for ( Pair<String, Value> fieldInRecord : record.fields() ) {
          msg.append( fieldInRecord.key() + " = " + fieldInRecord.value() + "\n" );
        }
      }

      return new InterpreterResult(Code.SUCCESS, msg.toString());

    } catch (Neo4jException ex) {
      logger.error("Cannot run " + cmd, ex);
      return new InterpreterResult(Code.ERROR, ex.getMessage());
    }
  }

  @Override public Scheduler getScheduler() {
    return SchedulerFactory.singleton()
        .createOrGetFIFOScheduler(Neo4jCypherInterpreter.class.getName() + this.hashCode());
  }

  @Override public int getProgress(InterpreterContext context) {
    return 0;
  }

  @Override
  public void destroy() {
    super.destroy();
    this.close();
  }

  @Override public FormType getFormType() {
    return FormType.SIMPLE;
  }

  @Override public void cancel(InterpreterContext context) {
    logger.info("Cannot cancel current query statement.");
  }

  @Override public List<String> completion(String buf, int cursor) {
    return NO_COMPLETION;
  }
}
