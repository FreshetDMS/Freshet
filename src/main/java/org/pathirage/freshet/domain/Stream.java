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
import java.util.List;

@Entity
@Table(name = "stream")
public class Stream extends BaseModel {

  @NotNull
  @Column(unique = true, nullable = false, updatable = false)
  private String identifier;

  @NotNull
  @Column(nullable = false)
  private Integer partitionCount;

  @Column(name = "key_serde_factory")
  private String keySerdeFactory;

  @Column(name = "value_serde_factory")
  private String valueSerdeFactory;

  @ManyToOne(optional = false)
  @Column(name = "system")
  private StorageSystem system;

  public Stream() {
  }

  public String getIdentifier() {
    return identifier;
  }

  public void setIdentifier(String identifier) {
    this.identifier = identifier;
  }

  public Integer getPartitionCount() {
    return partitionCount;
  }

  public void setPartitionCount(Integer partitionCount) {
    this.partitionCount = partitionCount;
  }

  public String getKeySerdeFactory() {
    return keySerdeFactory;
  }

  public void setKeySerdeFactory(String keySerdeFactory) {
    this.keySerdeFactory = keySerdeFactory;
  }

  public String getValueSerdeFactory() {
    return valueSerdeFactory;
  }

  public void setValueSerdeFactory(String valueSerdeFactory) {
    this.valueSerdeFactory = valueSerdeFactory;
  }

  public StorageSystem getSystem() {
    return system;
  }

  public void setSystem(StorageSystem system) {
    this.system = system;
  }
}
