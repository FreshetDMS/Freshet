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
@Table(name = "job")
public class Job extends BaseModel {

  @Column(nullable = false, unique = true, updatable = false)
  private String identifier;

  @ManyToMany
  @JoinTable(name = "job_input",
      joinColumns = @JoinColumn(name = "job", referencedColumnName = "id"),
      inverseJoinColumns = @JoinColumn(name = "input", referencedColumnName = "id"))
  private List<Stream> inputs = new ArrayList<>();

  @ManyToMany
  @JoinTable(name = "job_output",
      joinColumns = @JoinColumn(name = "job", referencedColumnName = "id"),
      inverseJoinColumns = @JoinColumn(name = "output", referencedColumnName = "id"))
  private List<Stream> outputs = new ArrayList<>();

  @ManyToOne
  private Topology topology;

  public void addInput(Stream input) {
    inputs.add(input);
  }

  public void addOutput(Stream output) {
    outputs.add(output);
  }

  public String getIdentifier() {
    return identifier;
  }

  public List<Stream> getInputs() {
    return inputs;
  }

  public List<Stream> getOutputs() {
    return outputs;
  }

  public void setIdentifier(String identifier) {
    this.identifier = identifier;
  }

  public Topology getTopology() {
    return topology;
  }

  public void setTopology(Topology topology) {
    this.topology = topology;
  }
}