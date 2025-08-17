package com.github.michaelzhao820.distributedlog.cmd.server;
import com.github.michaelzhao820.distributedlog.internal.server.Log;
import com.github.michaelzhao820.distributedlog.internal.server.LogHttpServer;


public class Main {
    public static void main(String[] args) throws Exception {
        var log = new Log();
        var server = new LogHttpServer(log, "localhost", 8080);
        server.start();
        System.out.println("Server started on http://localhost:8080");
    }
}
