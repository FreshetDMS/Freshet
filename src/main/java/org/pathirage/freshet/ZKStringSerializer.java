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

import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

import java.io.UnsupportedEncodingException;

public class ZKStringSerializer implements ZkSerializer {

  @Override
  public byte[] serialize(Object data) throws ZkMarshallingError {
    try {
      return ((String) data).getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new ZkMarshallingError(e);
    }
  }

  @Override
  public Object deserialize(byte[] bytes) throws ZkMarshallingError {
    if (bytes == null) {
      return null;
    }
    try {
      return new String(bytes, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      throw new ZkMarshallingError(e);
    }
  }
}

