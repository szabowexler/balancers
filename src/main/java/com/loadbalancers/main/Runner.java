package com.loadbalancers.main;

import com.google.protobuf.Service;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.loadbalancers.balancer.LoadBalancer;
import com.loadbalancers.balancer.client.LocalTracePlaybackClient;
import com.loadbalancers.balancer.impl.leastloaded.LeastLoadedBalancerImpl;
import com.loadbalancers.balancer.impl.random.RandomLoadBalancerImpl;
import com.loadbalancers.balancer.impl.roundrobin.RoundRobinLoadBalancerImpl;
import com.loadbalancers.balancer.worker.SimulationWorkerImpl;
import com.loadbalancers.balancer.worker.Worker;
import com.loadbalancers.logging.BalancerLogger;
import com.loadbalancers.logging.EventLogger;
import com.loadbalancers.logging.Logs;
import com.loadbalancers.rpc.server.RpcServer;
import com.loadbalancers.rpc.server.RpcServerFactory;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Elias Szabo-Wexler
 * @since 08/April/2015
 */
public class Runner {
    private final static Logger log = LogManager.getLogger(Runner.class);
    private final static List<Worker> workers = new ArrayList<>();

    public static void main (final String [] args) throws Exception {
        log.info("---------------------------------------------------");
        final Logs.BalancerType bType = Logs.BalancerType.valueOf(args[0]);
        log.info("Beginning run, using:\t" + bType);

        final ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("spring-config.xml");
        final Configs conf = context.getBean(Configs.class);
        final List<PeerInfo> workerInfos = spinUpWorkers(context);
        final RpcServer balancer = spinUpBalancer(context, workerInfos, bType);
        final LocalTracePlaybackClient client = makeClient(conf);
        final List<LoadBalancer.Trace> traces = loadTraces(context.getBean(Configs.class));
        traces.forEach(t -> {
            EventLogger.initialize(t, bType);
            workers.forEach(Worker::repeatBoot);
            client.runTrace(t);
        });
        System.exit(0);
    }

    protected static List<PeerInfo> spinUpWorkers (final ApplicationContext ctx) {
        final Configs conf = ctx.getBean(Configs.class);
        final int workerCount = conf.getWorkerCount();
        final RpcServerFactory serverFact = ctx.getBean(RpcServerFactory.class);
        final LinkedList<PeerInfo> workerInfo = new LinkedList<>();
        int port = 16418;
        for (int i = 0; i < workerCount; i ++ ) {
            log.info("Spinning up worker on port:\t" + port + ".");
            workerInfo.add(new PeerInfo("localhost", port));
            final RpcServer worker = serverFact.makeServer("localhost", port++, 50);
            final Worker w = new SimulationWorkerImpl();
            final Service loadBalancerWorkerService = LoadBalancer.LoadBalancerWorker.newReflectiveService(w);
            workers.add(w);
            worker.register(loadBalancerWorkerService);
            worker.runNonblocking();
        }
        return workerInfo;
    }

    protected static RpcServer spinUpBalancer (final ApplicationContext ctx,
                                               final List<PeerInfo> workers,
                                               final Logs.BalancerType t) {
        log.info("Spinning up balancer on port:\t" + 15418 + ".");
        final RpcServerFactory serverFact = ctx.getBean(RpcServerFactory.class);
        final RpcServer server = serverFact.makeServer("localhost", 15418, 250);


        final Service loadBalancerService;
        switch(t) {
            case LEAST_LOADED:
                loadBalancerService =
                        LoadBalancer.LoadBalancerServer.newReflectiveService(new LeastLoadedBalancerImpl(workers, "localhost", 15419));
                BalancerLogger.logBalancerType(Logs.BalancerType.LEAST_LOADED);
                break;
            case ROUND_ROBIN:
                loadBalancerService =
                        LoadBalancer.LoadBalancerServer.newReflectiveService(new RoundRobinLoadBalancerImpl(workers, "localhost", 15419));
                BalancerLogger.logBalancerType(Logs.BalancerType.ROUND_ROBIN);
                break;
            case RANDOM:
                loadBalancerService =
                        LoadBalancer.LoadBalancerServer.newReflectiveService(new RandomLoadBalancerImpl(workers, "localhost", 15419));
                BalancerLogger.logBalancerType(Logs.BalancerType.RANDOM);
                break;
            default:
                throw new IllegalArgumentException("Can't instantiate " + t + ".");
        }

        server.register(loadBalancerService);
        server.runNonblocking();
        return server;
    }

    protected static LocalTracePlaybackClient makeClient (final Configs conf) {
        final String balancerHostname = conf.getBalancerHostname();
        final int balancerPort = conf.getBalancerPort();
        final PeerInfo balancer = new PeerInfo(balancerHostname, balancerPort);
        return new LocalTracePlaybackClient(balancer, conf.getClientHostname(), conf.getClientPort());
    }

    public static List<LoadBalancer.Trace> loadTraces (final Configs conf) throws IOException{
        final File traceDir = Paths.get(conf.getTraceDir()).toFile();
        final ArrayList<LoadBalancer.Trace> traces = new ArrayList<>();
        for (final File f : traceDir.listFiles()) {
            log.info("Loading trace from file:\t" + f);
            final FileInputStream traceInputStream = new FileInputStream(f);
            final LoadBalancer.Trace trace = LoadBalancer.Trace.parseFrom(traceInputStream);
            traces.add(trace);
        }
        return traces;
    }
}
