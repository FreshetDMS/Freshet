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

import org.apache.samza.serializers.SerdeFactory;
import org.pathirage.freshet.api.Stream;
import org.pathirage.freshet.api.System;

public class KafkaTopic implements Stream {
  private final String name;
  private final int partitionCount;
  private final Class keyType;
  private final Class valueType;
  private final System system;

  private SerdeResolver serdeResolver = SerdeResolver.INSTANCE;

  public KafkaTopic(System system, String name, int partitionCount, Class keyType, Class valueType) {
    this.system = system;
    this.name = name;
    this.partitionCount = partitionCount;
    this.keyType = keyType;
    this.valueType = valueType;
  }

  public String getName() {
    return name;
  }

  public int getPartitionCount() {
    return partitionCount;
  }

  public Class<? extends SerdeFactory> getKeySerdeFactory() {
    return serdeResolver.getSerdeFactory(keyType);
  }

  public Class<? extends SerdeFactory> getValueSerdeFactory() {
    return serdeResolver.getSerdeFactory(valueType);
  }

  public System getSystem() {
    return system;
  }
}
