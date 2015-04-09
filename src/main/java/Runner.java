import balancer.server.SimpleLoadBalancerServer;
import client.LocalTracePlaybackClient;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * @author Elias Szabo-Wexler
 * @since 08/April/2015
 */
public class Runner {
    private final static Logger log = LogManager.getLogger(Runner.class);
    public static void main (final String [] args) throws Exception {
        log.info("Beginning run.");
        new Thread() {
            public void run () {
                try {
                    SimpleLoadBalancerServer.runServer();
                } catch (Exception ex) {
                    log.error("Error in server thread.");
                    ex.printStackTrace();
                }
            }
        }.start();
        log.info("Simple load balancer server online.");
        new LocalTracePlaybackClient().runTrace();
    }
}
