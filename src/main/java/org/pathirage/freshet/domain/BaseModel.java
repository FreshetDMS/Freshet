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

import io.ebean.Model;
import io.ebean.annotation.CreatedTimestamp;
import io.ebean.annotation.UpdatedTimestamp;

import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.MappedSuperclass;
import javax.persistence.Version;
import java.sql.Timestamp;

@MappedSuperclass
public class BaseModel extends Model {

  @Id
  @GeneratedValue
  Long id;

  @Version
  Long version;

  @CreatedTimestamp
  Timestamp whenCreated;

  @UpdatedTimestamp
  Timestamp whenUpdated;

  public Long getId() {
    return id;
  }

  public Long getVersion() {
    return version;
  }

  public Timestamp getWhenCreated() {
    return whenCreated;
  }

  public Timestamp getWhenUpdated() {
    return whenUpdated;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public void setVersion(Long version) {
    this.version = version;
  }

  public void setWhenCreated(Timestamp whenCreated) {
    this.whenCreated = whenCreated;
  }

  public void setWhenUpdated(Timestamp whenUpdated) {
    this.whenUpdated = whenUpdated;
  }
}
