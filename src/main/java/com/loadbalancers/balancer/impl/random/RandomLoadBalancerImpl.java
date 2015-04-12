package com.loadbalancers.balancer.impl.random;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.loadbalancers.balancer.LoadBalancer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * @author Elias Szabo-Wexler
 * @since 08/April/2015
 */

public class RandomLoadBalancerImpl implements LoadBalancer.LoadBalancerServer.Interface{

    private final static Logger log = LogManager.getLogger(RandomLoadBalancerImpl.class);

    @Override
    public void makeRequest(final RpcController controller,
                            final LoadBalancer.ClientRequest request,
                            final RpcCallback<LoadBalancer.ClientResponse> done) {
        try {
            log.info("Random load balancer received message:\t" + request);
            final LoadBalancer.ClientResponse.Builder respBuilder = LoadBalancer.ClientResponse.newBuilder();
            respBuilder.setResponse("Responding!");
            final LoadBalancer.ClientResponse resp = respBuilder.build();
            done.run(resp);
        } catch (Exception ex) {
            log.error("Server exception:\t" + ex);
            ex.printStackTrace();
        }
    }
}
