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

import org.pathirage.freshet.api.System;

public class KafkaSystem implements System {
  private final String name;
  private final String brokers;
  private final String zk;

  public KafkaSystem(String name, String brokers, String zk) {
    this.name = name;
    this.brokers = brokers;
    this.zk = zk;
  }

  @Override
  public String getName() {
    return name;
  }

  public String getBrokers() {
    return brokers;
  }

  public String getZk() {
    return zk;
  }
}
