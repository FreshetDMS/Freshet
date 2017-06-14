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

import org.pathirage.freshet.api.Visitor;

import java.util.ArrayList;
import java.util.List;

public class Node {
  private final String id;
  private final Object value;
  private final Type type;
  private final List<Node> inputs = new ArrayList<>();

  public Node(String id, Object value, Type type) {
    this.id = id;
    this.value = value;
    this.type = type;
  }

  public static enum Type {
    SOURCE,
    SINK,
    OPERATOR,
    INTERMEDIATE_STREAM;
  }

  public void addInput(Node input) {
    this.inputs.add(input);
  }

  public void removeInput(Node input) {
    this.inputs.remove(input);
  }

  public String getId() {
    return id;
  }

  public Object getValue() {
    return value;
  }

  public Type getType() {
    return type;
  }

  public List<Node> getInputs() {
    return inputs;
  }

  public void childrenAccept(Visitor visitor) {
    if (inputs.isEmpty()) {
      return;
    }

    int i = 0;
    for (Node input : inputs) {
      visitor.visit(input, i, this);
      i++;
    }
  }

  public int deriveDownstreamPartitionCount() {
    // Assume all inputs have same partition count
    // TODO: How to handle unequal partition counts in input
    if (type == Type.OPERATOR) {
      List<Integer> inputPartitionCounts = new ArrayList<>();
      for (Node n : inputs) {
        inputPartitionCounts.add(n.deriveDownstreamPartitionCount());
      }

      if (!areAllElementsEqual(inputPartitionCounts)){
        throw new IllegalStateException("Not all inputs derive same partition count.");
      } else {
        return inputPartitionCounts.get(0);
      }
    }

    return ((KafkaTopic)value).getPartitionCount();
  }

  private boolean areAllElementsEqual(List<Integer> intList) {
    Integer previous = null;

    for(Integer i : intList) {
      if (previous == null) {
        previous = i;
      }

      if (!previous.equals(i)) {
        return false;
      }

      previous = i;
    }

    return true;
  }
}
