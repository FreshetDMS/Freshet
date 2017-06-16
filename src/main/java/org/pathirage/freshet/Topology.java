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

import org.apache.samza.job.StreamJobFactory;
import org.pathirage.freshet.api.System;

import java.util.List;
import java.util.Map;

public abstract class Topology {
  protected final String name;
  protected final Map<String, Node> nodes;
  protected final List<String> sources;
  protected final List<String> sinks;
  protected final Class<? extends StreamJobFactory> jobFactoryClass;
  protected final System defaultSystem;
  protected final String host;
  protected final int port;

  protected Topology(String name, Map<String, Node> nodes, List<String> sources, List<String> sinks,
                     System defaultSystem, Class<? extends StreamJobFactory> jobFactoryClass, String host, int port) {
    this.name = name;
    this.nodes = nodes;
    this.sources = sources;
    this.sinks = sinks;
    this.defaultSystem = defaultSystem;
    this.jobFactoryClass = jobFactoryClass;
    this.host = host;
    this.port = port;
  }

  public abstract void submit();

  public abstract void visualize(String outputPath);

  public Map<String, Node> getNodes() {
    return nodes;
  }

  public List<String> getSources() {
    return sources;
  }

  public List<String> getSinks() {
    return sinks;
  }

  public String getName(){
    return name;
  }
}
