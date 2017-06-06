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

import java.util.List;
import java.util.Map;

public class Topology {
  private final Map<String, Node> nodes;
  private final List<String> sources;
  private final List<String> sinks;

  public Topology(Map<String, Node> nodes, List<String> sources, List<String> sinks) {
    this.nodes = nodes;
    this.sources = sources;
    this.sinks = sinks;
  }

  public void run() {
    // Get the sink
    // Do a depth traversal and come backwards while deploying all the necessary samza jobs
  }
}
