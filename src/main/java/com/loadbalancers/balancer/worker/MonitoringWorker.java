package com.loadbalancers.balancer.worker;

import com.google.common.util.concurrent.Uninterruptibles;
import com.loadbalancers.logging.Logs;
import com.loadbalancers.logging.WorkerLogger;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;

/**
 * @author Elias Szabo-Wexler
 * @since 19/April/2015
 */
public abstract class MonitoringWorker extends Worker {
    private final static Logger log = LogManager.getLogger(MonitoringWorker.class);
    public final static int LOAD_REPORT_INTERVAL_MS = 500;

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
        log.info("Logging system load.");
        final Logs.LoadSnapshot.Builder snapshotB = Logs.LoadSnapshot.newBuilder();

        // TODO: implement actual load monitoring here

        WorkerLogger.logLoad(snapshotB.build());
    }
}
