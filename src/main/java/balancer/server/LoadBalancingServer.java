package balancer.server;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import comm.LoadBalancer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * @author Elias Szabo-Wexler
 * @since 08/April/2015
 */

public class LoadBalancingServer implements LoadBalancer.LoadBalancerServer.Interface{

    private final static Logger log = LogManager.getLogger(LoadBalancingServer.class);

    @Override
    public void makeRequest(final RpcController controller,
                            final LoadBalancer.ClientRequest request,
                            final RpcCallback<LoadBalancer.ClientResponse> done) {
        log.info("Received message:\t" + request);
        throw new UnsupportedOperationException("Haven't implemented the server yet.");
    }
}
