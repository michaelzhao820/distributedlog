package com.github.michaelzhao820.distributedlog.internal.server;

import io.grpc.Server;

public class GrpcServerBuilder {
    private final CommitLog commitLog;
    private int port = 50051;

    public GrpcServerBuilder(CommitLog commitLog) {
        this.commitLog = commitLog;
    }

    public GrpcServerBuilder withPort(int port) {
        this.port = port;
        return this;
    }

    public Server build() {
        return io.grpc.ServerBuilder.forPort(port)
                .addService(new LogServerImpl(commitLog))
                .build();
    }
}
