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

import io.ebean.Ebean;

import java.util.Optional;

public class PersistenceUtils {

  public static Topology findTopologyByName(String name) throws EntityNotFoundException {
    Optional<Topology> topologyOptional = Ebean.find(Topology.class)
        .where().eq("name", name)
        .findOneOrEmpty();

    if (topologyOptional.isPresent()) {
      return topologyOptional.get();
    }

    throw new EntityNotFoundException("Cannot find topology with name '" + name + "'.");
  }

  public static StorageSystem findSystemByName(String name) throws EntityNotFoundException {
    Optional<StorageSystem> systemOptional = Ebean.find(StorageSystem.class)
        .where().eq("identifier", name)
        .findOneOrEmpty();

    if (systemOptional.isPresent()) {
      return systemOptional.get();
    }

    throw new EntityNotFoundException("Cannot find storage system with identifier '" + name + "'.");
  }

  public static boolean isSystemExists(String name) {
    return Ebean.find(StorageSystem.class)
        .where().eq("identifier", name)
        .findOneOrEmpty().isPresent();
  }

  public static boolean isStreamExists(String name, String systemName) {
    StorageSystem system;
    try {
      system = findSystemByName(systemName);
    } catch (EntityNotFoundException e) {
      throw new RuntimeException("Cannot find system with name '" + systemName + "'.");
    }

    return Ebean.find(Stream.class)
        .where()
        .eq("identifier", name)
        .eq("system", system)
        .findOneOrEmpty().isPresent();
  }

  public static class EntityNotFoundException extends Exception {
    public EntityNotFoundException(String message) {
      super(message);
    }
  }
}
