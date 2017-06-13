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

public class SamzaTopology extends Topology {

  protected SamzaTopology(String name, Map<String, Node> nodes, List<String> sources, List<String> sinks, System defaultSystem, Class<? extends StreamJobFactory> jobFactoryClass) {
    super(name, nodes, sources, sinks, defaultSystem, jobFactoryClass);
  }

  @Override
  public void run() {
    // Make sure source streams are available
    // Create sink streams
    // Derive intermediate streams
    // Generate configuration for each process node and run each process node as a Samza job with proper order
    // Handling metadata storage and job monitoring
    //   - Each topology get registered with Freshet manager
    //   - Manager knows the entire topology including source/sink streams, coordinator streams, metrics streams and yarn job ids
    //   - Manager provides a REST API
    //   - Manager periodically monitor jobs and take scaling decisions
    // Do we need to store the job graph in database? May be we generate intermediate streams and just keep a list of jobs with input/output stream tagged by topology id
    // How we can make sure output of a one task is compatible with input of a another task in topology? May be just assume in the prototype
    // We need to find upstream and downstream operators during scaling, so it should be possible to find them through what ever the information stored in database
  }

  @Override
  public void visualize(String outputPath) {

  }
}
