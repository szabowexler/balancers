package com.loadbalancers.balancer.worker;

import com.google.common.util.concurrent.Uninterruptibles;
import com.loadbalancers.logging.Logs;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * @author Elias Szabo-Wexler
 * @since 19/April/2015
 */
public abstract class MonitoringWorker extends Worker {
    private final static Logger log = LogManager.getLogger(MonitoringWorker.class);
    public final static int LOAD_REPORT_INTERVAL_MS = 50;

    public MonitoringWorker() {
        new MonitorThread().start();
    }

    class MonitorThread extends Thread {
        public MonitorThread () {
            super("Worker-Monitor-Thread");
        }

        public void run() {
            while (true) {
                Uninterruptibles.sleepUninterruptibly(LOAD_REPORT_INTERVAL_MS, TimeUnit.MILLISECONDS);
                reportLoad();
            }
        }
    }

    protected void reportLoad () {
        final Logs.LoadSnapshot.Builder snapshotB = Logs.LoadSnapshot.newBuilder();

        final int queue = reqs.size();
        final int numRunning = threadPool.getActiveCount();
        final int max = logicalCores;

        snapshotB.setWorkerQueueSize(queue);
        snapshotB.setWorkerCurrentJobs(numRunning);
        snapshotB.setWorkerMaxConcurrentJobs(max);

        workerLogger.logLoad(snapshotB.build());
    }
}
