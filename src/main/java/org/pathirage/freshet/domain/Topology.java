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
package org.pathirage.freshet.domain;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "topology")
public class Topology extends BaseModel {

  @Column(length = 100)
  private String name;

  @ManyToMany(cascade = CascadeType.PERSIST)
  @JoinTable(name = "topology_source",
      joinColumns = @JoinColumn(name = "topology", referencedColumnName = "id"),
      inverseJoinColumns = @JoinColumn(name = "source", referencedColumnName = "id"))
  private List<Stream> sources = new ArrayList<>();

  @ManyToMany(cascade = CascadeType.PERSIST)
  @JoinTable(name = "topology_sink",
      joinColumns = @JoinColumn(name = "topology", referencedColumnName = "id"),
      inverseJoinColumns = @JoinColumn(name = "sink", referencedColumnName = "id"))
  private List<Stream> sinks = new ArrayList<>();

  @OneToMany(mappedBy = "topology", cascade = CascadeType.PERSIST)
  private List<Job> jobs = new ArrayList<>();

  @Transient
  private List<Job> currentStage = new ArrayList<>();

  public List<Stream> getSources() {
    return sources;
  }

  public List<Stream> getSinks() {
    return sinks;
  }

  public List<Job> getJobs() {
    return jobs;
  }

  public void addJob(Job job) {
    jobs.add(job);
  }

  public void addSource(Stream stream) {
    sources.add(stream);
  }

  public void addSink(Stream stream) {
    sinks.add(stream);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  /**
   * IT looks like we don't need stage iteration feature. We just need a way to find a jobs upstream and downstream.
   * Recursive scheduling technique used in Spark can be utilized to schedule jobs.
   */


  public boolean hasNextStage() {
    if (currentStage.isEmpty() && hasJobsConsumingSources()) {
      return true;
    } else if (isAllCurrentJobsPublishToSinks()) {
      return false;
    } else if (currentStageHasDownstreamJobs()) {
      return true;
    }
    return false;
  }

  public List<Job> nextStage() {
    if (currentStage.isEmpty() && hasJobsConsumingSources()) {
      currentStage = getInputStage();
      return currentStage;
    } else {
      currentStage = getDownstreamOfCurrentStage();
      if (!currentStage.isEmpty()) {
        return currentStage;
      }
    }

    return new ArrayList<>();
  }

  private List<Job> getDownstreamOfCurrentStage() {
    List<Stream> outputs = new ArrayList<>();
    List<Job> downstreamStage = new ArrayList<>();

    for (Job job : currentStage) {
      outputs.addAll(job.getOutputs());
    }

    for (Job job : jobs) {
      for (Stream input : job.getInputs()) {
        if (outputs.contains(input)) {
          downstreamStage.add(job);
        }
      }
    }

    return downstreamStage;
  }

  private List<Job> getInputStage() {
    List<Job> inputStage = new ArrayList<>();
    for (Job job : jobs) {
      if (isAllJobInputsSources(job.getInputs())) {
        inputStage.add(job);
      }
    }

    return inputStage;
  }

  private boolean hasJobsConsumingSources() {
    for (Job job : jobs) {
      if (isAllJobInputsSources(job.getInputs())) {
        return true;
      }
    }

    return false;
  }

  private boolean currentStageHasDownstreamJobs() {
    for (Job job : currentStage) {
      if (hasConsumerJobs(job.getOutputs())) {
        return true;
      }
    }

    return false;
  }

  private boolean hasConsumerJobs(List<Stream> streams) {
    for (Job job : jobs) {
      for (Stream stream : streams) {
        if (job.getInputs().contains(stream)) {
          return true;
        }
      }
    }

    return false;
  }

  private boolean isAllCurrentJobsPublishToSinks() {
    for (Job job : currentStage) {
      if (!areAllJobOutputsSinks(job.getOutputs())) {
        return false;
      }
    }

    return true;
  }

  private boolean areAllJobOutputsSinks(List<Stream> jobOutputs) {
    for (Stream output : jobOutputs) {
      if (!sinks.contains(output)) {
        return false;
      }
    }

    return true;
  }

  private boolean isAllJobInputsSources(List<Stream> jobInputs) {
    for (Stream input : jobInputs) {
      if (!sources.contains(input)) {
        return false;
      }
    }

    return false;
  }
}
