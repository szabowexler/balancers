package com.loadbalancers.balancer.impl;

import com.google.protobuf.ServiceException;
import com.googlecode.protobuf.pro.duplex.ClientRpcController;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.loadbalancers.balancer.LoadBalancer;
import com.loadbalancers.rpc.client.RpcClient;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Elias Szabo-Wexler
 * @since 09/April/2015
 */
public abstract class LoadBalancerImpl implements LoadBalancer.LoadBalancerServer.Interface{
    private final static Logger log = LogManager.getLogger(LoadBalancerImpl.class);
    protected final RpcClient workerClient;
    protected final Random r;
    protected int nextServer;
    protected Map<Integer, RpcClientChannel> workerIdToChannelMap;

    public LoadBalancerImpl (final List<PeerInfo> servers,
                             final String localHostname,
                             final int localPort) {
        workerClient = new RpcClient(servers, localHostname, localPort);
        workerIdToChannelMap = new ConcurrentHashMap<>();
        final AtomicInteger workerID = new AtomicInteger(0);
        workerClient.getChannels().forEach((server, channel) -> {
            int id = workerID.getAndIncrement();
            LoadBalancer.BalancerConfigurationRequest.Builder confBuilder = LoadBalancer.BalancerConfigurationRequest.newBuilder();
            confBuilder.setWorkerID(id);
            final LoadBalancer.BalancerConfigurationRequest req = confBuilder.build();

            final LoadBalancer.LoadBalancerWorker.BlockingInterface worker = LoadBalancer.LoadBalancerWorker.newBlockingStub(channel);
            final ClientRpcController workerController = channel.newRpcController();
            workerController.setTimeoutMs(0);

            try {
                worker.setID(workerController, req);
                workerIdToChannelMap.put(id, channel);
                log.info("Assigned worker " + id + " to their ID.");
            } catch (ServiceException ex) {
                log.error("Something went wrong setting a worker id.", ex);
            }
        });
        this.r = new Random(System.currentTimeMillis());
        this.nextServer = 0;
    }

    public RpcClientChannel getRandomServer () {
        final List<RpcClientChannel> servers = workerClient.getRpcClientRegistry().getAllClients();
        if (workerIdToChannelMap.isEmpty()) {
            throw new IllegalStateException("Error: no workers available!");
        } else {
            int workerID = r.nextInt(servers.size());
            return workerIdToChannelMap.get(workerID);
        }
    }

    public RpcClientChannel getNextServer () {
        if (workerIdToChannelMap.isEmpty()) {
            throw new IllegalStateException("Error: no workers available!");
        } else {
            if (nextServer >= workerIdToChannelMap.size()) {
                nextServer = 0;
            }
            return workerIdToChannelMap.get(nextServer++);
        }
    }
}
