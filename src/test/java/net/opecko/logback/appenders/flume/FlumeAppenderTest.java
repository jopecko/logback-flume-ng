/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache license, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the license for the specific language governing permissions and
 * limitations under the license.
 */
package net.opecko.logback.appenders.flume;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;

import org.apache.flume.Channel;
import org.apache.flume.ChannelException;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.lifecycle.LifecycleController;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.source.AvroSource;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * @author Joe O'Pecko
 */
public class FlumeAppenderTest {

  private static LoggerContext ctx;
  private static final int testServerPort = 12345;

  private AvroSource eventSource;
  private Channel channel;
  private Logger avroLogger;
  private String testPort;

  @BeforeClass
  public static void beforeAllTests() {
    ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
  }

  @AfterClass
  public static void afterAllTests() {
    ctx.getStatusManager().clear();
  }

  @Before
  public void beforeEachTest() throws Exception {
    eventSource = new AvroSource();
    channel = new MemoryChannel();
    Configurables.configure(channel, new Context());
    avroLogger = ctx.getLogger("avrologger");
    /*
     * Clear out all other appenders associated with this logger to ensure we're
     * only hitting the Avro appender.
     */
    avroLogger.detachAndStopAllAppenders();
    final Context context = new Context();
    testPort = String.valueOf(testServerPort);
    context.put("port", testPort);
    context.put("bind", "0.0.0.0");
    Configurables.configure(eventSource, context);
    final List<Channel> channels = new ArrayList<Channel>();
    channels.add(channel);
    final ChannelSelector cs = new ReplicatingChannelSelector();
    cs.setChannels(channels);
    eventSource.setChannelProcessor(new ChannelProcessor(cs));
    eventSource.start();
    assertThat(
      "Reached start or error",
      LifecycleController.waitForOneOf(eventSource, LifecycleState.START_OR_ERROR),
      is(true)
    );
    assertThat("Server is started", eventSource.getLifecycleState(), equalTo(LifecycleState.START));
  }

  @After
  public void afterEachTest() throws Exception {
    avroLogger.detachAndStopAllAppenders();
    eventSource.stop();
    final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    final Set<ObjectName> names = server.queryNames(new ObjectName("org.apache.flume.*:*"), null);
    for (final ObjectName name : names) {
      try {
        server.unregisterMBean(name);
      } catch (final Exception ex) {
        System.out.println("Unable to unregister " + name.toString());
      }
    }
    assertThat(
      "Reached stop or error",
      LifecycleController.waitForOneOf(eventSource, LifecycleState.STOP_OR_ERROR),
      is(true)
    );
    assertThat("Server is stopped", eventSource.getLifecycleState(), equalTo(LifecycleState.STOP));
  }

  @Test
  public void testLog4jAvroAppender() throws InterruptedException, IOException {
    avroLogger.addAppender(createAppender());
    avroLogger.setLevel(Level.ALL);

    assertThat(avroLogger, notNullValue());
    avroLogger.info("Test message");

    final Transaction transaction = channel.getTransaction();
    transaction.begin();
    final Event event = channel.take();
    assertThat(event, notNullValue());
    String message = getBody(event);
    System.out.println("HERE I AM: " + message);
    assertThat(
      "Channel contained event, but not expected message",
      getBody(event).endsWith("Test message"),
      is(true)
    );
    transaction.commit();
    transaction.close();
    eventSource.stop();
  }

  @Test
  public void testMultiple() throws InterruptedException, IOException {
    avroLogger.addAppender(createAppender());
    avroLogger.setLevel(Level.ALL);

    assertThat(avroLogger, notNullValue());

    for (int i = 0; i < 10; ++i) {
      avroLogger.info("Test message " + i);
    }

    for (int i = 0; i < 10; ++i) {
      final Transaction transaction = channel.getTransaction();
      transaction.begin();

      final Event event = channel.take();
      assertThat(event, notNullValue());
      assertThat(
        "Channel contained event, but not expected message",
        getBody(event).trim().endsWith("Test message " + i),
        is(true)
      );
      transaction.commit();
      transaction.close();
    }
    eventSource.stop();
  }

  @Test
  public void testBatch() throws InterruptedException, IOException {
    avroLogger.addAppender(createAppender());
    avroLogger.setLevel(Level.ALL);

    assertThat(avroLogger, notNullValue());

    for (int i = 0; i < 10; ++i) {
      avroLogger.info("Test message " + i);
    }

    final Transaction transaction = channel.getTransaction();
    transaction.begin();

    for (int i = 0; i < 10; ++i) {
      final Event event = channel.take();
      assertThat("No event for item " + i, event, notNullValue());
      assertThat(
        "Channel contained event, but not expected message",
        getBody(event).trim().endsWith("Test message " + i),
        is(true)
      );
    }
    transaction.commit();
    transaction.close();
    eventSource.stop();
  }

  @Test
  public void testReconnect() throws Exception {
    final String altPort = Integer.toString(Integer.parseInt(testPort) + 1);
    avroLogger.addAppender(createAppender(Agent.create("localhost", testPort),Agent.create("localhost", altPort)));
    avroLogger.setLevel(Level.ALL);

    avroLogger.info("Test message");

    Transaction transaction = channel.getTransaction();
    transaction.begin();

    Event event = channel.take();
    assertThat(avroLogger, notNullValue());
    assertThat("Channel contained event, but not expected message", getBody(event).endsWith("Test message"), is(true));
    transaction.commit();
    transaction.close();

    eventSource.stop();
    try {
      final Context context = new Context();
      context.put("port", altPort);
      context.put("bind", "0.0.0.0");

      Configurables.configure(eventSource, context);

      eventSource.start();
    } catch (final ChannelException e) {
      fail("Caught exception while resetting port to " + altPort + " : " + e.getMessage());
    }

    avroLogger.info("Test message 2");

    transaction = channel.getTransaction();
    transaction.begin();

    event = channel.take();
    assertThat(event, notNullValue());
    assertThat(
      "Channel contained event, but not expected message",
      getBody(event).endsWith("Test message 2"),
      is(true)
    );
    transaction.commit();
    transaction.close();
  }

//    @Test
//    public void testConnectionRefused() {
//      PatternLayout layout = new PatternLayout();
//      layout.setContext(ctx);
//      layout.setPattern("%d{yyyy-MM-dd HH:mm:ss} %c [%p] %m%n");
//      layout.start();
//
//      final FlumeAppender avroAppender = new FlumeAppender();
//      avroAppender.addAgent(Agent.create("localhost", testPort));
//      avroAppender.setCompressBody(true);
//      avroAppender.setType("avro");
//      avroAppender.setLayout(layout);
//      avroAppender.setName("FlumeAppender");
//      avroAppender.start();
//      avroLogger.addAppender(avroAppender);
//      avroLogger.setLevel(Level.ALL);
//      eventSource.stop();
//
//      boolean caughtException = false;
//
//      try {
//        avroLogger.info("message 1");
//      } catch (final Throwable t) {
//        //logger.debug("Logging to a non-existent server failed (as expected)", t);
//        caughtException = true;
//      }
//      assertTrue(caughtException);
//    }

//    @Test
//    public void testNotConnected() throws Exception {
//      eventSource.stop();
//      final String altPort = Integer.toString(Integer.parseInt(testPort) + 1);
//      final FlumeAppender avroAppender = createAppender(
//        Agent.create("localhost", testPort),
//        Agent.create("localhost", altPort)
//      );
//      assertTrue("Appender Not started", avroAppender.isStarted());
//      avroLogger.addAppender(avroAppender);
//      avroLogger.setLevel(Level.ALL);
//
//      try {
//        avroLogger.info("Test message");
//        fail("Exception should have been thrown");
//      } catch (final Exception e) {
//      }
//
//      try {
//        final Context context = new Context();
//        context.put("port", altPort);
//        context.put("bind", "0.0.0.0");
//        Configurables.configure(eventSource, context);
//        eventSource.start();
//      } catch (final ChannelException e) {
//        fail("Caught exception while resetting port to " + altPort + " : " + e.getMessage());
//      }
//
//      avroLogger.info("Test message 2");
//
//      final Transaction transaction = channel.getTransaction();
//      transaction.begin();
//
//      final Event event = channel.take();
//      assertNotNull(event);
//      assertTrue("Channel contained event, but not expected message", getBody(event).endsWith("Test message 2"));
//      transaction.commit();
//      transaction.close();
//    }

  private String getBody(final Event event) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final InputStream is = new GZIPInputStream(new ByteArrayInputStream(event.getBody()));
    int n = 0;
    while ((n = is.read()) != -1) {
      baos.write(n);
    }
    return new String(baos.toByteArray()).trim(); // FIXME - should I be calling trim() here?
  }

  private FlumeAppender createAppender(final Agent... agents) {
    PatternLayout layout = new PatternLayout();
    layout.setContext(ctx);
    layout.setPattern("%d{yyyy-MM-dd HH:mm:ss} %c [%p] %m%n");
    layout.start();

    final FlumeAppender avroAppender = new FlumeAppender();
    if (null == agents || agents.length == 0) {
      avroAppender.addAgent(Agent.create("localhost", testPort));
    } else {
      for (Agent agent : agents) {
        avroAppender.addAgent(agent);
      }
    }
    avroAppender.setCompressBody(true);
    avroAppender.setType("avro");
    avroAppender.setLayout(layout);
    avroAppender.setName("FlumeAppender");
    avroAppender.start();
    return avroAppender;
  }

}
