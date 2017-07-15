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

import org.pathirage.freshet.domain.*;

import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

public class NewTopologyHandlerTask implements Runnable {
  private final String topologyName;
  private final org.pathirage.freshet.domain.Topology topology;
  private final Queue<Job> deploymentQueue = new LinkedList<>();

  NewTopologyHandlerTask(String topologyName) throws PersistenceUtils.EntityNotFoundException {
    this.topologyName = topologyName;
    this.topology = PersistenceUtils.findTopologyByName(topologyName);
  }

  @Override
  public void run() {
    /*
     * Jobs to be deployed are kept in a queue
     *   - Check queue has any jobs. If queue is empty get the jobs that write to sinks and put them to queue
     *   - Pick a job from the queue and put input jobs to queue and deploy the picked job
     *   - Continue above step until the queue is empty
     */
    if (deploymentQueue.isEmpty()) {
      List<Job> rootJobs = topology.getJobsWritingToSinks();
      if (rootJobs.isEmpty()) {
        throw new IllegalStateException("Topology has no jobs writing only to sinks.");
      }

      deploymentQueue.addAll(rootJobs);
    }

    while(!deploymentQueue.isEmpty()) {
      Job currentJob = deploymentQueue.remove();
      deploymentQueue.addAll(topology.getPublishers(currentJob.getInputs()));
      // deploy the current job. We need to know the constraints to such as partition counts of input topics.z
    }
  }
}
