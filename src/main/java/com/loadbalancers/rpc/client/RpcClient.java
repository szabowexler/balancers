package com.loadbalancers.rpc.client;

import com.googlecode.protobuf.pro.duplex.*;
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientPipelineFactory;
import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.logging.NullLogger;
import com.googlecode.protobuf.pro.duplex.server.RpcClientRegistry;
import com.googlecode.protobuf.pro.duplex.timeout.RpcTimeoutChecker;
import com.googlecode.protobuf.pro.duplex.timeout.RpcTimeoutExecutor;
import com.googlecode.protobuf.pro.duplex.timeout.TimeoutChecker;
import com.googlecode.protobuf.pro.duplex.timeout.TimeoutExecutor;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Elias Szabo-Wexler
 * @since 08/April/2015
 */

public class RpcClient {
    private final static Logger log = LogManager.getLogger(RpcClient.class);

    protected final PeerInfo client;
    protected final List<PeerInfo> servers;
    protected final Map<PeerInfo, RpcClientChannel> channels;
    protected final Map<PeerInfo, ClientRpcController> controllers;
    protected final DuplexTcpClientPipelineFactory clientFactory;

    public RpcClient (final List<PeerInfo> servers,
                      final String localHostname,
                      final int localPort) {
        client = new PeerInfo(localHostname, localPort);
        this.servers = servers;
        this.channels = new ConcurrentHashMap<>();
        this.controllers = new ConcurrentHashMap<>();

        final RpcServerCallExecutor executor = new ThreadPoolCallExecutor(3, 100);
        clientFactory = makeClientFactory(client, executor);
        final CleanShutdownHandler cleanShutdownHandler = makeTimeoutChecker(clientFactory, executor);

        RpcConnectionEventNotifier rpcEventNotifier = new RpcConnectionEventNotifier();
        RpcConnectionEventListener listener = new ClientRpcConnectionEventListener();
        rpcEventNotifier.setEventListener(listener);
        clientFactory.registerConnectionEventListener(rpcEventNotifier);

        final Bootstrap bootstrap = makeClientBootstrap(clientFactory, cleanShutdownHandler);

        servers.forEach(server -> {
            try {
                log.info("Connecting from " + client + " to :\t" + server);
                final RpcClientChannel rpcClient = clientFactory.peerWith(server, bootstrap);
                channels.put(server, rpcClient);
            } catch (Exception ex) {
                log.error("Something went wrong connecting to:\t" + server, ex);
                System.exit(-1);
            }
        });
    }

    protected DuplexTcpClientPipelineFactory makeClientFactory (final PeerInfo client,
                                                                final RpcServerCallExecutor executor) {
        final DuplexTcpClientPipelineFactory clientFactory = new DuplexTcpClientPipelineFactory();
        clientFactory.setClientInfo(client); // forces a local port nr.

        clientFactory.setConnectResponseTimeoutMillis(1000);
        clientFactory.setRpcServerCallExecutor(executor);
        clientFactory.setCompression(false);

        final NullLogger logger = new NullLogger();
        clientFactory.setRpcLogger(logger);

        return clientFactory;
    }

    protected CleanShutdownHandler makeTimeoutChecker (final DuplexTcpClientPipelineFactory clientFactory,
                                       final RpcServerCallExecutor executor) {
        final RpcTimeoutExecutor timeoutExecutor = new TimeoutExecutor(1,5);
        final RpcTimeoutChecker checker = new TimeoutChecker();
        checker.setTimeoutExecutor(timeoutExecutor);
        checker.startChecking(clientFactory.getRpcClientRegistry());

        final CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();
        shutdownHandler.addResource(executor);
        shutdownHandler.addResource(checker);
        shutdownHandler.addResource(timeoutExecutor);

        return shutdownHandler;
    }

    protected Bootstrap makeClientBootstrap (final DuplexTcpClientPipelineFactory clientFactory,
                                             final CleanShutdownHandler shutdownHandler) {
        final Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(new NioEventLoopGroup());
        bootstrap.handler(clientFactory);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS,10000);
        bootstrap.option(ChannelOption.SO_SNDBUF, 1048576);
        bootstrap.option(ChannelOption.SO_RCVBUF, 1048576);

        shutdownHandler.addResource(bootstrap.group());
        return bootstrap;
    }

    public RpcClientRegistry getRpcClientRegistry() {
        return clientFactory.getRpcClientRegistry();
    }

    public Map<PeerInfo, RpcClientChannel> getChannels() {
        return channels;
    }
}
