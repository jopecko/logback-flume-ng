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
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import ch.qos.logback.classic.LoggerContext;

import com.google.common.base.Preconditions;

import org.apache.avro.AvroRemoteException;
import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Responder;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.flume.source.avro.AvroSourceProtocol;
import org.apache.flume.source.avro.Status;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * @author Sojern
 */
public class FlumeAvroAppenderTest {

  private static final String CONFIG = "avro.xml";
  private static final String HOSTNAME = "localhost";
  private static LoggerContext ctx;

  private EventCollector primary;
  private EventCollector alternate;

  @BeforeClass
  public static void beforeAllTests() {
    final File file = new File("target/file-channel");
    if (!deleteFiles(file)) {
        System.err.println("Warning - unable to delete target/file-channel. Test errors may occur");
    }
  }

  @AfterClass
  public static void afterAllTests() {
    ctx.getStatusManager().clear();
  }

  @Before
  public void beforeEachTest() throws Exception {
    final File file = new File("target/file-channel");
    final boolean result = deleteFiles(file);

    /*
    * Clear out all other appenders associated with this logger to ensure we're
    * only hitting the Avro appender.
    */
    final int[] ports = findFreePorts(2);
    System.setProperty("primaryPort", Integer.toString(ports[0]));
    System.setProperty("alternatePort", Integer.toString(ports[1]));
    primary = new EventCollector(ports[0]);
    alternate = new EventCollector(ports[1]);
//    System.setProperty(XMLConfigurationFactory.CONFIGURATION_FILE_PROPERTY, CONFIG);
    ctx = (LoggerContext) LoggerFactory.getILoggerFactory();
    ctx.reset();
  }

  @After
  public void afterEachTest() throws Exception {
//      System.clearProperty(XMLConfigurationFactory.CONFIGURATION_FILE_PROPERTY);
    ctx.reset();
    primary.stop();
    alternate.stop();
    final File file = new File("target/file-channel");
    final boolean result = deleteFiles(file);
    final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
    final Set<ObjectName> names = server.queryNames(new ObjectName("org.apache.flume.*:*"), null);
    for (final ObjectName name : names) {
      try {
        server.unregisterMBean(name);
      } catch (final Exception ex) {
        System.out.println("Unable to unregister " + name.toString());
      }
    }
  }

//  @Test
//  public void testLogbackEvent() throws InterruptedException, IOException {
//    final StructuredDataMessage msg = new StructuredDataMessage("Test", "Test Logback", "Test");
//    EventLogger.logEvent(msg);
//
//    final Event event = primary.poll();
//    Assert.assertNotNull(event);
//    final String body = getBody(event);
//    Assert.assertTrue("Channel contained event, but not expected message. Received: " + body, body.endsWith("Test Logback"));
//  }

  private static class EventCollector implements AvroSourceProtocol {

    private final LinkedBlockingQueue<AvroFlumeEvent> eventQueue;
    private final NettyServer nettyServer;

    public EventCollector(final int port) {
      this.eventQueue = new LinkedBlockingQueue<AvroFlumeEvent>();
      final Responder responder = new SpecificResponder(AvroSourceProtocol.class, this);
      nettyServer = new NettyServer(responder, new InetSocketAddress(HOSTNAME, port));
      nettyServer.start();
    }

    public void stop() {
      nettyServer.close();
    }

    public Event poll() {

      AvroFlumeEvent avroEvent = null;
      try {
        avroEvent = eventQueue.poll(30000, TimeUnit.MILLISECONDS);
      } catch (final InterruptedException ie) {
        // Ignore the exception.
      }
      if (avroEvent != null) {
        return EventBuilder.withBody(avroEvent.getBody().array(), toStringMap(avroEvent.getHeaders()));
      } else {
        System.out.println("No Event returned");
      }
      return null;
    }

    @Override
    public Status append(final AvroFlumeEvent event) throws AvroRemoteException {
      eventQueue.add(event);
      return Status.OK;
    }

    @Override
    public Status appendBatch(final List<AvroFlumeEvent> events)
        throws AvroRemoteException {
        Preconditions.checkState(eventQueue.addAll(events));
        return Status.OK;
    }
  }

  private static Map<String, String> toStringMap(final Map<CharSequence, CharSequence> charSeqMap) {
    final Map<String, String> stringMap = new HashMap<String, String>();
    for (final Map.Entry<CharSequence, CharSequence> entry : charSeqMap.entrySet()) {
        stringMap.put(entry.getKey().toString(), entry.getValue().toString());
    }
    return stringMap;
  }

  private static int[] findFreePorts(final int count) throws IOException {
    final int[] ports = new int[count];
    final ServerSocket[] sockets = new ServerSocket[count];
    try {
        for (int i = 0; i < count; ++i) {
            sockets[i] = new ServerSocket(0);
            ports[i] = sockets[i].getLocalPort();
        }
    } finally {
        for (int i = 0; i < count; ++i) {
            if (sockets[i] != null) {
                try {
                    sockets[i].close();
                } catch (final Exception ex) {
                    // Ignore the error.
                }
            }
        }
    }
    return ports;
  }

  private static boolean deleteFiles(final File file) {
    boolean result = true;
    if (file.isDirectory()) {

      final File[] files = file.listFiles();
      for (final File child : files) {
        result &= deleteFiles(child);
      }

    } else if (!file.exists()) {
      return true;
    }
    return result &= file.delete();
  }

  private String getBody(final Event event) throws IOException {
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    final InputStream is = new GZIPInputStream(new ByteArrayInputStream(event.getBody()));
    int n = 0;
    while ((n = is.read()) != -1) {
      baos.write(n);
    }
    return new String(baos.toByteArray()).trim(); // FIXME - should I be calling trim() here?
  }

}
