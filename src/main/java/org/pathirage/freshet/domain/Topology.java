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

  public static TopologyFinder find = new TopologyFinder();

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
}
