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

import io.ebean.annotation.NotNull;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;

@Entity
@Table(name = "system")
public class StorageSystem extends BaseModel {
  @NotNull
  @Column(unique = true, nullable = false)
  private String identifier;

  @OneToMany(mappedBy = "system")
  private List<Stream> streams = new ArrayList<>();

  @OneToMany(mappedBy = "system", cascade = CascadeType.PERSIST)
  private List<StorageSystemProperty> properties = new ArrayList<>();

  public String getIdentifier() {
    return identifier;
  }

  public void setIdentifier(String identifier) {
    this.identifier = identifier;
  }

  public List<Stream> getStreams() {
    return streams;
  }

  public void addStream(Stream stream) {
    this.streams.add(stream);
  }

  public List<StorageSystemProperty> getProperties() {
    return properties;
  }

  public void addProperty(StorageSystemProperty property) {
    properties.add(property);
  }
}
