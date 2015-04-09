package client;

import com.google.protobuf.RpcCallback;
import com.googlecode.protobuf.pro.duplex.*;
import com.googlecode.protobuf.pro.duplex.client.DuplexTcpClientPipelineFactory;
import com.googlecode.protobuf.pro.duplex.execute.RpcServerCallExecutor;
import com.googlecode.protobuf.pro.duplex.execute.ThreadPoolCallExecutor;
import com.googlecode.protobuf.pro.duplex.listener.RpcConnectionEventListener;
import com.googlecode.protobuf.pro.duplex.logging.NullLogger;
import com.googlecode.protobuf.pro.duplex.timeout.RpcTimeoutChecker;
import com.googlecode.protobuf.pro.duplex.timeout.RpcTimeoutExecutor;
import com.googlecode.protobuf.pro.duplex.timeout.TimeoutChecker;
import com.googlecode.protobuf.pro.duplex.timeout.TimeoutExecutor;
import comm.LoadBalancer;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelOption;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * @author Elias Szabo-Wexler
 * @since 08/April/2015
 */
public class LocalTracePlaybackClient {
    private final static Logger log = LogManager.getLogger(LocalTracePlaybackClient.class);
    protected List<LoadBalancer.ClientRequest> trace;

    public void runTrace () {
        try {
            doThing();
        } catch (Exception ex) {
            log.error(ex);
            ex.printStackTrace();
        }
    }

    protected void doThing () throws Exception {
        String serverHostname = "localhost";
        int serverPort = 15418;
        String clientHostname = "localhost";
        int clientPort = 15419;
        boolean secure = false;
        boolean nodelay = true;
        boolean compress = true;

        log.info("DuplexPingPongClient port=" + clientPort  +
                " ssl=" + (secure?"Y":"N") +
                " nodelay=" + (nodelay?"Y":"N")+
                " compress=" +(compress?"Y":"N"));

        PeerInfo client = new PeerInfo(clientHostname, clientPort);
        PeerInfo server = new PeerInfo(serverHostname, serverPort);

        RpcServerCallExecutor executor = new ThreadPoolCallExecutor(3, 100 );

        DuplexTcpClientPipelineFactory clientFactory = new DuplexTcpClientPipelineFactory();
        clientFactory.setClientInfo(client); // forces a local port nr.

        clientFactory.setConnectResponseTimeoutMillis(10000);
        clientFactory.setRpcServerCallExecutor(executor);
        clientFactory.setCompression(compress);
        if ( secure ) {
            RpcSSLContext sslCtx = new RpcSSLContext();
            sslCtx.setKeystorePassword("changeme");
            sslCtx.setKeystorePath("./lib/client.keystore");
            sslCtx.setTruststorePassword("changeme");
            sslCtx.setTruststorePath("./lib/truststore");
            sslCtx.init();

            clientFactory.setSslContext(sslCtx);
        }

        NullLogger logger = new NullLogger();
        clientFactory.setRpcLogger(logger);

        RpcTimeoutExecutor timeoutExecutor = new TimeoutExecutor(1,5);
        RpcTimeoutChecker checker = new TimeoutChecker();
        checker.setTimeoutExecutor(timeoutExecutor);
        checker.startChecking(clientFactory.getRpcClientRegistry());

        CleanShutdownHandler shutdownHandler = new CleanShutdownHandler();
        shutdownHandler.addResource(executor);
        shutdownHandler.addResource(checker);
        shutdownHandler.addResource(timeoutExecutor);

        // setup a RPC event listener - it just logs what happens
        RpcConnectionEventNotifier rpcEventNotifier = new RpcConnectionEventNotifier();
        RpcConnectionEventListener listener = new RpcConnectionEventListener() {

            @Override
            public void connectionReestablished(RpcClientChannel clientChannel) {
                log.info("connectionReestablished " + clientChannel);
            }

            @Override
            public void connectionOpened(RpcClientChannel clientChannel) {
                log.info("connectionOpened " + clientChannel);
            }

            @Override
            public void connectionLost(RpcClientChannel clientChannel) {
                log.info("connectionLost " + clientChannel);
            }

            @Override
            public void connectionChanged(RpcClientChannel clientChannel) {
                log.info("connectionChanged " + clientChannel);
            }
        };
        rpcEventNotifier.setEventListener(listener);
        clientFactory.registerConnectionEventListener(rpcEventNotifier);

        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(new NioEventLoopGroup());
        bootstrap.handler(clientFactory);
        bootstrap.channel(NioSocketChannel.class);
        bootstrap.option(ChannelOption.TCP_NODELAY, nodelay);
        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS,10000);
        bootstrap.option(ChannelOption.SO_SNDBUF, 1048576);
        bootstrap.option(ChannelOption.SO_RCVBUF, 1048576);

        shutdownHandler.addResource(bootstrap.group());

        try {
            clientFactory.peerWith(server, bootstrap);

            final List<RpcClientChannel> allServers = clientFactory.getRpcClientRegistry().getAllClients();
            if (allServers.size() == 0) {
                log.error("Error: no servers available!");
            } else {
                final RpcClientChannel channel = allServers.get(0);
                call(channel);
            }
        } catch( Throwable e ) {
            log.error("Throwable.", e);
        } finally {
            System.exit(0);
        }
    }

    protected static void call (final RpcClientChannel channel) {
        try {

            LoadBalancer.LoadBalancerServer.Interface nonBlockingService = LoadBalancer.LoadBalancerServer.newStub(channel);
            final ClientRpcController controller = channel.newRpcController();
            controller.setTimeoutMs(1000);

            // we set a Oob response callback even if we dont request percentComplete messages
            // to be able to test if we receive any when we didn't ask.
            LoadBalancer.ClientRequest.Builder reqBuilder = LoadBalancer.ClientRequest.newBuilder();
            reqBuilder.setJobID(0)
                    .setSendTime(0)
                    .setT(LoadBalancer.JobType.TIMED_JOB);
            final LoadBalancer.ClientRequest req = reqBuilder.build();

            // we expect to cancel the call at 50% of the processing duration of the ping.
            // which preceeds any pong call from the server side.
            RpcCallback<LoadBalancer.ClientResponse> callback = new RpcCallback<LoadBalancer.ClientResponse>() {

                @Override
                public void run(LoadBalancer.ClientResponse res) {
                    log.info("We got a " + res);
                    if ( res == null ) {
                        controller.storeCallLocalVariable("failure", Boolean.TRUE);
                    } else {
                        controller.storeCallLocalVariable("failure", Boolean.FALSE);
                        controller.storeCallLocalVariable("res", res);
                    }
                }
            };

            nonBlockingService.makeRequest(controller, req, callback);
        } catch ( Throwable t) {
            log.error(t);
        }
    }
}
