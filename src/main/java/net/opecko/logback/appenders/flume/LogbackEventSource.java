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

import org.apache.flume.ChannelException;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogbackEventSource extends AbstractSource implements EventDrivenSource {

  private static final Logger LOGGER = LoggerFactory.getLogger(LogbackEventSource.class);

  private final SourceCounter sourceCounter = new SourceCounter("logback");

  public LogbackEventSource() {
    setName("LogbackEvent");
  }

  @Override
  public synchronized void start() {
    super.start();
    LOGGER.info("Logback Source started");
  }

  @Override
  public synchronized void stop() {
    super.stop();
    LOGGER.info("Logback Source stopped. Metrics {}", sourceCounter);
  }

  public void send(final Event event) {
    sourceCounter.incrementAppendReceivedCount();
    sourceCounter.incrementEventReceivedCount();
    try {
      getChannelProcessor().processEvent(event);
    } catch (final ChannelException ex) {
      throw ex;
    }
    sourceCounter.incrementAppendAcceptedCount();
    sourceCounter.incrementEventAcceptedCount();
  }

}
