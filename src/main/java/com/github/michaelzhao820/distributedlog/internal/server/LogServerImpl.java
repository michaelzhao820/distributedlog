package com.github.michaelzhao820.distributedlog.internal.server;

import com.github.michaelzhao820.distributedlog.api.v1.LogGrpc;
import com.github.michaelzhao820.distributedlog.api.v1.LogProto.ProduceRequest;
import com.github.michaelzhao820.distributedlog.api.v1.LogProto.ProduceResponse;
import com.github.michaelzhao820.distributedlog.api.v1.LogProto.ConsumeRequest;
import com.github.michaelzhao820.distributedlog.api.v1.LogProto.ConsumeResponse;
import com.github.michaelzhao820.distributedlog.api.v1.LogProto.Record;

import io.grpc.stub.StreamObserver;
import io.grpc.Status;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.IOException;


public class LogServerImpl extends LogGrpc.LogImplBase {

    private static final Logger logger = Logger.getLogger(LogServerImpl.class.getName());
    private final CommitLog commitLog;

    public LogServerImpl(CommitLog commitLog) {
        this.commitLog = commitLog;
        logger.info("LogServerImpl initialized");
    }

    @Override
    public void produce(ProduceRequest request, StreamObserver<ProduceResponse> responseObserver) {
        logger.fine("Received produce request: " + request);
        try {
            long offset = commitLog.append(request.getRecord());
            ProduceResponse response = ProduceResponse.newBuilder()
                    .setOffset(offset)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            logger.fine("Produce successful, offset: " + offset);
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error while producing record", e);
            responseObserver.onError(e);
        }
    }

    @Override
    public void consume(ConsumeRequest request, StreamObserver<ConsumeResponse> responseObserver) {
        logger.fine("Received consume request for offset: " + request.getOffset());
        try {
            Record record = commitLog.read(request.getOffset());
            ConsumeResponse response = ConsumeResponse.newBuilder()
                    .setRecord(record)
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
            logger.fine("Consume successful for offset: " + request.getOffset());
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error while consuming record", e);
            responseObserver.onError(Status.OUT_OF_RANGE
                    .withDescription(e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Unexpected error while consuming record", e);
            responseObserver.onError(Status.UNKNOWN
                    .withDescription(e.getMessage())
                    .withCause(e)
                    .asRuntimeException());
        }
    }

    @Override
    public StreamObserver<ProduceRequest> produceStream(StreamObserver<ProduceResponse> responseObserver) {
        logger.info("Produce stream opened");
        return new StreamObserver<>() {
            @Override
            public void onNext(ProduceRequest produceRequest) {
                logger.fine("Streaming produce request: " + produceRequest);
                try {
                    long offset = commitLog.append(produceRequest.getRecord());
                    ProduceResponse response = ProduceResponse.newBuilder()
                            .setOffset(offset)
                            .build();
                    responseObserver.onNext(response);
                    logger.fine("Streaming produce successful, offset: " + offset);
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Error during streaming produce", e);
                    throw new RuntimeException(e);
                }
            }

            @Override
            public void onError(Throwable throwable) {
                logger.log(Level.WARNING, "Stream encountered an error", throwable);
            }

            @Override
            public void onCompleted() {
                responseObserver.onCompleted();
                logger.info("Produce stream completed");
            }
        };
    }

    @Override
    public void consumeStream(ConsumeRequest request, StreamObserver<ConsumeResponse> responseObserver) {
        AtomicLong offset = new AtomicLong(request.getOffset());
        logger.info("Consume stream opened starting at offset: " + request.getOffset());

        new Thread(() -> {
            try {
                while (true) {
                    if (((io.grpc.stub.ServerCallStreamObserver<?>) responseObserver).isCancelled()) {
                        logger.info("Consume stream cancelled by client at offset: " + offset.get());
                        break;
                    }

                    try {
                        Record record = commitLog.read(offset.get());
                        if (record == null) {
                            // offset not available yet, retry
                            logger.fine("Offset " + offset.get() + " not available yet, retrying...");
                            Thread.sleep(10);
                            continue;
                        }

                        ConsumeResponse response = ConsumeResponse.newBuilder()
                                .setRecord(record)
                                .build();
                        responseObserver.onNext(response);
                        logger.fine("Streaming consume sent record at offset: " + offset.get());
                        offset.incrementAndGet(); // increment only after successful send
                    } catch (Exception e) {
                        logger.log(Level.SEVERE, "Error during streaming consume at offset: " + offset.get(), e);
                        responseObserver.onError(e);
                        break;
                    }
                }
            } finally {
                responseObserver.onCompleted();
                logger.info("Consume stream completed for client starting at offset: " + request.getOffset());
            }
        }).start();
    }
}
