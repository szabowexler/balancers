package com.loadbalancers.main;

import com.google.common.util.concurrent.Uninterruptibles;
import com.google.protobuf.Service;
import com.googlecode.protobuf.pro.duplex.PeerInfo;
import com.loadbalancers.balancer.LoadBalancer;
import com.loadbalancers.balancer.client.LocalTracePlaybackClient;
import com.loadbalancers.balancer.impl.random.RandomLoadBalancerImpl;
import com.loadbalancers.balancer.worker.SimulationWorkerImpl;
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
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author Elias Szabo-Wexler
 * @since 08/April/2015
 */
public class Runner {
    private final static Logger log = LogManager.getLogger(Runner.class);
    public static void main (final String [] args) throws Exception {
        log.info("---------------------------------------------------");
        log.info("Beginning run.");
        final ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext("spring-config.xml");
        final Configs conf = context.getBean(Configs.class);
        spinUpWorkers(context);
        final RpcServer balancer = spinUpBalancer(context);
        final LocalTracePlaybackClient client = makeClient(conf);
        final List<LoadBalancer.Trace> traces = loadTraces(context.getBean(Configs.class));
        traces.forEach(client::runTrace);

        while (true) {
            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS);
        }
    }

    protected static void spinUpWorkers (final ApplicationContext ctx) {
        final Configs conf = ctx.getBean(Configs.class);
        final int workerCount = conf.getWorkerCount();
        final RpcServerFactory serverFact = ctx.getBean(RpcServerFactory.class);
        int port = 15419;
        for (int i = 0; i < workerCount; i ++ ) {
            log.info("Spinning up worker on port:\t" + port + ".");
            final RpcServer worker = serverFact.makeServer("localhost", port++);
            final Service loadBalancerWorkerService = LoadBalancer.LoadBalancerWorker.newReflectiveService(new SimulationWorkerImpl());
            worker.register(loadBalancerWorkerService);
            worker.runNonblocking();
        }
    }

    protected static RpcServer spinUpBalancer (final ApplicationContext ctx) {
        log.info("Spinning up balancer on port:\t" + 15418 + ".");
        final RpcServerFactory serverFact = ctx.getBean(RpcServerFactory.class);
        final RpcServer server = serverFact.makeServer("localhost", 15418);
        final Service loadBalancerService = LoadBalancer.LoadBalancerServer.newReflectiveService(new RandomLoadBalancerImpl());
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

    protected static List<LoadBalancer.Trace> loadTraces (final Configs conf) throws IOException{
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
