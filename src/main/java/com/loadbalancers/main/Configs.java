package com.loadbalancers.main;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/**
 * @author Elias Szabo-Wexler
 * @since 09/April/2015
 */

@Component
public class Configs {
    protected @Value("${balancer.server.hostname}") String balancerHostname;
    protected @Value("${balancer.server.port}") int balancerPort;

    protected @Value("${worker.server.count}") int workerCount;

    protected @Value("${client.hostname}") String clientHostname;
    protected @Value("${client.port}") int clientPort;

    protected @Value("${traces.dir}") String traceDir;

    public String getBalancerHostname() {
        return balancerHostname;
    }

    public int getBalancerPort() {
        return balancerPort;
    }

    public String getClientHostname() {
        return clientHostname;
    }

    public int getClientPort() {
        return clientPort;
    }

    public String getTraceDir() {
        return traceDir;
    }

    public int getWorkerCount() {
        return workerCount;
    }
}
