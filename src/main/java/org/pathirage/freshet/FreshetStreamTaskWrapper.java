/**
 * Copyright 2017 Milinda Pathirage
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.pathirage.freshet;

import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.task.*;


public class FreshetStreamTaskWrapper implements StreamTask, InitableTask, WindowableTask {
  @Override
  public void init(Config config, TaskContext context) throws Exception {

  }

  @Override
  public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) throws Exception {

  }

  @Override
  public void window(MessageCollector collector, TaskCoordinator coordinator) throws Exception {

  }
}
