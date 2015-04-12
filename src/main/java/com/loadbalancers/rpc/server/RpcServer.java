package com.loadbalancers.rpc.server;

import com.google.protobuf.Service;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerPipelineFactory;
import io.netty.bootstrap.ServerBootstrap;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Elias Szabo-Wexler
 * @since 08/April/2015
 */
public class RpcServer {
    private static Logger log = LogManager.getLogger(RpcServer.class);

    protected final ServerBootstrap bootstrap;
    protected final DuplexTcpServerPipelineFactory serverFactory;
    protected final AtomicBoolean running = new AtomicBoolean(false);

    public RpcServer(ServerBootstrap bootstrap, DuplexTcpServerPipelineFactory serverFactory) {
        this.bootstrap = bootstrap;
        this.serverFactory = serverFactory;
    }

    public void register (final Service S) {
        serverFactory.getRpcServiceRegistry().registerService(true, S);
    }

    public void runBlocking () {
        run();
    }

    public void runNonblocking () {
        final RpcServer thisServer = this;
        log.info("Launching server [nonblocking]...");
        new Thread () {
            public void run () {
                thisServer.run();
            }
        }.start();

        // Wait for server to be launched
        while (!running.get()) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ex) {}
        }

        log.info("Server running! Returning to calling thread.");
    }

    protected void run () {
        bootstrap.bind();
        log.info("Serving " + bootstrap + ".");
        running.set(true);

        while ( true ) {
            List<RpcClientChannel> clients = serverFactory.getRpcClientRegistry().getAllClients();
            log.info("Number of clients="+ clients.size());

            try {
                Thread.sleep(5000);
            } catch (InterruptedException ex) {
                log.warn("Something interrupted our sleep:\t" + ex, ex);
            }
        }
    }
}
