package com.loadbalancers.rpc.server;

import com.google.protobuf.ExtensionRegistry;
import com.googlecode.protobuf.pro.duplex.CleanShutdownHandler;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcConnectionEventNotifier;
import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.logging.CategoryPerServiceLogger;
import com.googlecode.protobuf.pro.duplex.logging.RpcLogger;
import com.googlecode.protobuf.pro.duplex.server.DuplexTcpServerPipelineFactory;
import com.googlecode.protobuf.pro.duplex.util.RenamingThreadFactoryProxy;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.log4j.Logger;
import org.springframework.stereotype.Component;

import java.util.concurrent.Executors;

/**
 * @author Elias Szabo-Wexler
 * @since 09/April/2015
 */
@Component
public class RpcServerFactory {
    private final static Logger log = Logger.getLogger(RpcServer.class);
//    protected @Value("${balancer.server.hostname}") String hostname;
//    protected @Value("${balancer.server.port}") int port;

    public RpcServer makeServer (final String hostname, final int port) {
        log.info("Creating server on: <" + hostname + ":" + port + ">.");
        final PeerInfo serverInfo = new PeerInfo(hostname, port);
        final DuplexTcpServerPipelineFactory serverFactory = makeServerFactory(serverInfo);
        log.info("Created server tcp factory.");
        final RpcServerCallExecutor rpcExecutor = new ThreadPoolCallExecutor(10, 10);
        serverFactory.setRpcServerCallExecutor(rpcExecutor);
        log.info("Created server executor.");
        final ServerBootstrap serverBootstrap = makeBootstrap(serverFactory, rpcExecutor, serverInfo);
        log.info("Created server bootstrap.");
        return new RpcServer(serverBootstrap, serverFactory);
    }

    protected DuplexTcpServerPipelineFactory makeServerFactory (final PeerInfo serverInfo) {
        DuplexTcpServerPipelineFactory serverFactory = new DuplexTcpServerPipelineFactory(serverInfo);
        final RpcLogger logger = makeRPCLogger();

        ExtensionRegistry r = ExtensionRegistry.newInstance();
        serverFactory.setExtensionRegistry(r);

        serverFactory.setLogger(logger);

        RpcConnectionEventNotifier rpcEventNotifier = new RpcConnectionEventNotifier();
        RpcConnectionEventListener listener = new ServerRpcConnectionEventListener();
        rpcEventNotifier.setEventListener(listener);
        serverFactory.registerConnectionEventListener(rpcEventNotifier);

        return serverFactory;
    }

    protected RpcLogger makeRPCLogger () {
        final CategoryPerServiceLogger logger = new CategoryPerServiceLogger();
//        logger.setLogRequestProto(false);
//        logger.setLogResponseProto(false);
        return logger;
    }

    protected ServerBootstrap makeBootstrap(final DuplexTcpServerPipelineFactory serverFactory,
                                            final RpcServerCallExecutor rpcExecutor,
                                            final PeerInfo serverInfo) {
        final ServerBootstrap bootstrap = new ServerBootstrap();
        final EventLoopGroup boss = new NioEventLoopGroup(2,new RenamingThreadFactoryProxy("boss", Executors.defaultThreadFactory()));
        final EventLoopGroup workers = new NioEventLoopGroup(2,new RenamingThreadFactoryProxy("worker", Executors.defaultThreadFactory()));

        bootstrap.group(boss,workers);
        bootstrap.channel(NioServerSocketChannel.class);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.option(ChannelOption.SO_SNDBUF, 1048576);
        bootstrap.option(ChannelOption.SO_RCVBUF, 1048576);
        bootstrap.childOption(ChannelOption.SO_RCVBUF, 1048576);
        bootstrap.childOption(ChannelOption.SO_SNDBUF, 1048576);
        bootstrap.option(ChannelOption.TCP_NODELAY, false);
        bootstrap.childHandler(serverFactory);
        bootstrap.localAddress(serverInfo.getPort());

        CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();
        shutdownHandler.addResource(boss);
        shutdownHandler.addResource(workers);
        shutdownHandler.addResource(rpcExecutor);

        return bootstrap;
    }

}
