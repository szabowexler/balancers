package com.loadbalancers.balancer.impl.random;

import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.googlecode.protobuf.pro.duplex.RpcClientChannel;
import com.loadbalancers.balancer.impl.DynamicLoadBalancerImpl;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.Collection;
import java.util.List;

/**
 * @author Elias Szabo-Wexler
 * @since 08/April/2015
 */

public class RandomLoadBalancerImpl extends DynamicLoadBalancerImpl {

    private final static Logger log = LogManager.getLogger(RandomLoadBalancerImpl.class);

    public RandomLoadBalancerImpl(List<PeerInfo> servers, String localHostname, int localPort) {
        super(servers, localHostname, localPort);
    }

    @Override
    protected int newWorkerID() {
        final Collection<RpcClientChannel> servers = workerClient.getChannels().values();
        if (workerIdToChannelMap.isEmpty()) {
            throw new IllegalStateException("Error: no workers available!");
        } else {
            return r.nextInt(servers.size());
        }
    }
}
