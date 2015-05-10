package com.loadbalancers.logging;

import com.loadbalancers.balancer.LoadBalancer;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Elias Szabo-Wexler
 * @since 02/May/2015
 */
public class EventLogger {
    private final static Logger log = LogManager.getLogger(EventLogger.class);
    private final static String logDir = "logs";
    private final static String logFileSuffix = "logevents.log";
    public static FileOutputStream out;
    protected static boolean initialized = false;

    public static void initialize (final LoadBalancer.Trace t,
                                   final Logs.BalancerType type) {
        try {
            final File f = getFileForTrace(t, type);
            out = new FileOutputStream(f);
            log.info("Logging " + t.getTraceName() + " to:\t" + f);
            initialized = true;
            BalancerLogger.logBalancerType(type);
        } catch (IOException ex) {
            out = null;
            throw new RuntimeException(ex);
        }
    }

    protected static File getFileForTrace (final LoadBalancer.Trace t,
                                           final Logs.BalancerType type) {
        final String fileName = t.getTraceName() + "." + type.toString() + "." + logFileSuffix;
        return Paths.get(logDir, fileName).toFile();
    }

    protected synchronized static void logEvent (final Logs.LogEvent.Builder builder) {
        if (!initialized) return;
        builder.setTime(System.currentTimeMillis());
        final Logs.LogEvent e = builder.build();
        try {
            e.writeDelimitedTo(out);
            out.flush();
        } catch (IOException ex) {
            log.error("Failed to write log event to string!", ex);
        }
    }

    public static List<Logs.LogEvent> readEventLog (final File f) throws IOException {
        final FileInputStream str = new FileInputStream(f);
        final ArrayList<Logs.LogEvent> events = new ArrayList<>();

        Logs.LogEvent e = Logs.LogEvent.parseDelimitedFrom(str);
        while (e != null) {
            try {
                events.add(e);
                e = Logs.LogEvent.parseDelimitedFrom(str);
            } catch (Exception ex) {
                ex.printStackTrace();
                throw new RuntimeException(ex);
            }
        }

        return events;
    }
}
