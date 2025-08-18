package com.github.michaelzhao820.distributedlog.internal.server;

import com.github.michaelzhao820.distributedlog.api.v1.LogGrpc;
import com.github.michaelzhao820.distributedlog.api.v1.LogProto;
import com.github.michaelzhao820.distributedlog.api.v1.LogProto.*;
import com.github.michaelzhao820.distributedlog.internal.log.Log;
import com.github.michaelzhao820.distributedlog.internal.log.Config;
import com.google.protobuf.ByteString;
import io.grpc.*;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

class LogServerImplTest {

    private Server server;
    private ManagedChannel channel;
    private LogGrpc.LogBlockingStub blockingStub;
    private LogGrpc.LogStub asyncStub;
    private Path tempDir;

    @BeforeEach
    void setup() throws IOException {
        Config config = new Config();
        config.segment.initialOffset = 0;
        config.segment.maxStoreBytes = 1024;
        config.segment.maxIndexBytes = 1024;

        // Create a temporary directory for the log
        tempDir = Files.createTempDirectory("server-test");
        Log commitLog = new Log(tempDir.toString(), config);

        String serverName = InProcessServerBuilder.generateName();
        server = InProcessServerBuilder.forName(serverName)
                .directExecutor()
                .addService(new LogServerImpl(commitLog))
                .build()
                .start();

        channel = InProcessChannelBuilder.forName(serverName)
                .directExecutor()
                .build();

        blockingStub = LogGrpc.newBlockingStub(channel);
        asyncStub = LogGrpc.newStub(channel);
    }

    @AfterEach
    void teardown() throws IOException {
        if (channel != null) channel.shutdownNow();
        if (server != null) server.shutdownNow();

        // Delete the temporary directory
        if (tempDir != null) {
            Files.walk(tempDir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> p.toFile().delete());
        }
    }

    @Test
    void testProduceConsume() {
        LogProto.Record record = LogProto.Record.newBuilder()
                .setValue(ByteString.copyFromUtf8("hello world"))
                .build();

        ProduceResponse produceRes = blockingStub.produce(
                ProduceRequest.newBuilder().setRecord(record).build()
        );

        ConsumeResponse consumeRes = blockingStub.consume(
                ConsumeRequest.newBuilder().setOffset(produceRes.getOffset()).build()
        );

        assertEquals("hello world", consumeRes.getRecord().getValue().toStringUtf8());
        assertEquals(produceRes.getOffset(), consumeRes.getRecord().getOffset());
    }

    @Test
    void testConsumePastBoundary() {
        LogProto.Record record = LogProto.Record.newBuilder()
                .setValue(ByteString.copyFromUtf8("hello world"))
                .build();

        ProduceResponse produceRes = blockingStub.produce(
                ProduceRequest.newBuilder().setRecord(record).build()
        );

        StatusRuntimeException ex = assertThrows(
                StatusRuntimeException.class,
                () -> blockingStub.consume(
                        ConsumeRequest.newBuilder().setOffset(produceRes.getOffset() + 1).build()
                )
        );

        assertEquals(Status.Code.OUT_OF_RANGE, ex.getStatus().getCode());
    }

    @Test
    void testProduceConsumeStream() throws Exception {
        CountDownLatch produceLatch = new CountDownLatch(2);
        List<ProduceResponse> produceResponses = new ArrayList<>();

        StreamObserver<ProduceRequest> produceReqObs =
                asyncStub.produceStream(new StreamObserver<>() {
                    @Override
                    public void onNext(ProduceResponse value) {
                        produceResponses.add(value);
                        produceLatch.countDown();
                    }
                    @Override public void onError(Throwable t) { fail(t); }
                    @Override public void onCompleted() {}
                });

        LogProto.Record rec1 = LogProto.Record.newBuilder().setValue(ByteString.copyFromUtf8("first message")).build();
        LogProto.Record rec2 = LogProto.Record.newBuilder().setValue(ByteString.copyFromUtf8("second message")).build();

        produceReqObs.onNext(ProduceRequest.newBuilder().setRecord(rec1).build());
        produceReqObs.onNext(ProduceRequest.newBuilder().setRecord(rec2).build());
        produceReqObs.onCompleted();

        assertTrue(produceLatch.await(1, TimeUnit.SECONDS));
        assertEquals(0, produceResponses.get(0).getOffset());
        assertEquals(1, produceResponses.get(1).getOffset());

        CountDownLatch consumeLatch = new CountDownLatch(2);
        List<LogProto.Record> consumed = new ArrayList<>();

        asyncStub.consumeStream(
                ConsumeRequest.newBuilder().setOffset(0).build(),
                new StreamObserver<>() {
                    @Override
                    public void onNext(ConsumeResponse value) {
                        consumed.add(value.getRecord());
                        consumeLatch.countDown();
                    }
                    @Override public void onError(Throwable t) { fail(t); }
                    @Override public void onCompleted() {}
                }
        );

        assertTrue(consumeLatch.await(1, TimeUnit.SECONDS));
        assertEquals("first message", consumed.get(0).getValue().toStringUtf8());
        assertEquals("second message", consumed.get(1).getValue().toStringUtf8());
    }
}
