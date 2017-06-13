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

import org.apache.samza.job.StreamJobFactory;
import org.graphstream.graph.implementations.DefaultGraph;
import org.graphstream.stream.file.FileSinkImages;
import org.pathirage.freshet.api.System;
import org.pathirage.freshet.api.Visitor;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class VisualizableTopology extends Topology {


  protected VisualizableTopology(String name, Map<String, Node> nodes, List<String> sources, List<String> sinks, System defaultSystem, Class<? extends StreamJobFactory> jobFactoryClass) {
    super(name, nodes, sources, sinks, defaultSystem, jobFactoryClass);
  }

  @Override
  public void visualize(String outputPath) {
    for (String sink : sinks) {
      Node n = nodes.get(sink);
      TopologyVisualizer v = new TopologyVisualizer();
      v.visitRoot(n);

      FileSinkImages pic = new FileSinkImages(FileSinkImages.OutputType.PNG, FileSinkImages.Resolutions.TwoK);
      pic.setLayoutPolicy(FileSinkImages.LayoutPolicy.COMPUTED_FULLY_AT_NEW_IMAGE);
      try {
        pic.writeAll(v.getGraph(), outputPath);
      } catch (IOException e) {
        throw new RuntimeException("Could not visualize topology", e);
      }
    }

  }

  public static class TopologyVisualizer implements Visitor {
    private Node root;
    private final DefaultGraph g = new DefaultGraph("job-topology");


    public Node visitRoot(Node n) {
      root = n;
      visit(n, 0, null);
      return root;
    }

    public void visit(Node n, int ordinal, Node parent) {
      org.graphstream.graph.Node uiNode = g.addNode(n.getId());
      uiNode.addAttribute("ui.label", n.getId());
      n.childrenAccept(this);
      switch (n.getType()) {
        case SINK:
          break;
        case SOURCE:
        case OPERATOR:
          g.addEdge(String.format("%s-%s", n.getId(), parent.getId()), n.getId(), parent.getId(), true);
          break;
      }
    }

    public DefaultGraph getGraph() {
      return g;
    }
  }
}