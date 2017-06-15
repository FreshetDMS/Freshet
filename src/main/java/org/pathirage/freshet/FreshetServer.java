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

import io.ebean.Ebean;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.pathirage.freshet.domain.*;
import org.pathirage.freshet.domain.Topology;
import org.pathirage.freshet.grpc.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

public class FreshetServer {
  private final Logger logger = LoggerFactory.getLogger(FreshetServer.class);

  private Server server;

  private void start() throws IOException {
    int port = 50050;

    server = ServerBuilder.forPort(port)
        .addService(new FreshetImpl())
        .build()
        .start();

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        FreshetServer.this.stop();
        System.err.println("*** server shut down");
      }
    });
  }

  private void stop() {
    if (server != null) {
      server.shutdown();
    }
  }

  private void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    final FreshetServer freshetServer = new FreshetServer();
    freshetServer.start();
    freshetServer.blockUntilShutdown();
  }

  static class FreshetImpl extends FreshetGrpc.FreshetImplBase {
    @Override
    public void submitTopology(PBTopology request, StreamObserver<TopologySubmissionResponse> responseObserver) {
      Ebean.beginTransaction();

      Topology topology = new Topology();
      topology.setName(request.getName());
      topology.setStatus(org.pathirage.freshet.domain.Status.NEW);
      topology.save();

      try {
        for (PBStreamReference streamReference : request.getInputsList()) {
          if (!PersistenceUtils.isStreamExists(streamReference.getIdentifier(), streamReference.getSystem())) {
            throw new PersistenceUtils.EntityNotFoundException("Cannot find stream " + streamReference.getIdentifier() +
                " of system " + streamReference.getSystem());
          }

          Stream stream =
              PersistenceUtils.findStreamByName(streamReference.getIdentifier(), streamReference.getSystem());

          topology.addSource(stream);
        }

        for (PBStreamReference streamReference : request.getOutputsList()) {
          if (!PersistenceUtils.isStreamExists(streamReference.getIdentifier(), streamReference.getSystem())) {
            throw new PersistenceUtils.EntityNotFoundException("Cannot find stream " + streamReference.getIdentifier() +
                " of system " + streamReference.getSystem());
          }

          Stream stream =
              PersistenceUtils.findStreamByName(streamReference.getIdentifier(), streamReference.getSystem());

          topology.addSink(stream);
        }

        for (PBJob j : request.getJobsList()) {
          Job job = new Job();
          job.setTopology(topology);
          job.setIdentifier(j.getIdentifier());
          job.setOperator(j.getOperator().toByteArray());
          job.setStatus(org.pathirage.freshet.domain.Status.NEW);
          job.save();

          for(Map.Entry<String, String> prop : request.getPropertiesMap().entrySet()) {
            JobProperty jobProperty = new JobProperty(prop.getKey(), prop.getValue(), job);
            jobProperty.save();
            job.addProperty(jobProperty);
          }

          job.update();

          for (PBStreamReference streamReference : j.getInputsList()) {
            if (!PersistenceUtils.isStreamExists(streamReference.getIdentifier(), streamReference.getSystem())) {
              throw new PersistenceUtils.EntityNotFoundException("Cannot find stream " + streamReference.getIdentifier() +
                  " of system " + streamReference.getSystem());
            }

            Stream stream =
                PersistenceUtils.findStreamByName(streamReference.getIdentifier(), streamReference.getSystem());

            job.addInput(stream);
          }

          for (PBStreamReference streamReference : j.getOutputsList()) {
            if (!PersistenceUtils.isStreamExists(streamReference.getIdentifier(), streamReference.getSystem())) {
              throw new PersistenceUtils.EntityNotFoundException("Cannot find stream " + streamReference.getIdentifier() +
                  " of system " + streamReference.getSystem());
            }

            Stream stream =
                PersistenceUtils.findStreamByName(streamReference.getIdentifier(), streamReference.getSystem());

            job.addOutput(stream);
          }

          job.update();
          topology.addJob(job);
        }
        topology.update();
        Ebean.commitTransaction();

        responseObserver.onNext(TopologySubmissionResponse.newBuilder()
            .setId(topology.getId())
            .setName(topology.getName())
            .build());
        responseObserver.onCompleted();
      } catch (PersistenceUtils.EntityNotFoundException e) {
        responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage()).asRuntimeException());
      } finally {
        Ebean.endTransaction();
      }
    }

    @Override
    public void getTopology(GetTopologyRequest request, StreamObserver<PBTopology> responseObserver) {
      super.getTopology(request, responseObserver);
    }

    @Override
    public void registerSystem(PBSystem request, StreamObserver<SystemRegistrationResponse> responseObserver) {
      Ebean.beginTransaction();

      StorageSystem storageSystem = new StorageSystem();
      storageSystem.setIdentifier(request.getIdentifier());
      storageSystem.save();

      for (Map.Entry<String, String> props : request.getPropertiesMap().entrySet()) {
        StorageSystemProperty storageSystemProperty = new StorageSystemProperty(props.getKey(), props.getValue());
        storageSystemProperty.setSystem(storageSystem);
        storageSystemProperty.save();

        storageSystem.addProperty(storageSystemProperty);
      }

      storageSystem.update();

      Ebean.commitTransaction();
      Ebean.endTransaction();

      responseObserver.onNext(SystemRegistrationResponse.newBuilder()
          .setId(storageSystem.getId())
          .setIdentifier(storageSystem.getIdentifier())
          .build());
      responseObserver.onCompleted();
    }

    @Override
    public void registerStream(PBStream request, StreamObserver<StreamRegistrationResponse> responseObserver) {
      Ebean.beginTransaction();

      try {
        StorageSystem storageSystem = PersistenceUtils.findSystemByName(request.getSystem());
        Stream stream = new Stream();
        stream.setSystem(storageSystem);
        stream.setIdentifier(request.getIdentifier());
        stream.setPartitionCount(request.getPartitionCount());
        stream.setKeySerdeFactory(request.getKeySerdeFactory());
        stream.setValueSerdeFactory(request.getValueSerdeFactory());
        stream.save();

        storageSystem.addStream(stream);
        storageSystem.update();

        Ebean.commitTransaction();

        responseObserver.onNext(StreamRegistrationResponse.newBuilder()
            .setIdentifier(stream.getIdentifier())
            .setId(stream.getId())
            .build());
        responseObserver.onCompleted();

      } catch (PersistenceUtils.EntityNotFoundException e) {
        responseObserver.onError(Status.INTERNAL.withDescription(e.getMessage())
            .asRuntimeException());
      } finally {
        Ebean.endTransaction();
      }
    }
  }
}
