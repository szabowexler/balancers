package com.loadbalancers.analysis.events;

import java.util.*;

/**
 * @author Elias Szabo-Wexler
 * @since 22/March/2015
 */
public class LogEvent {
    public enum EventType {
        SERVER_WORKER_REQUESTED("REQUEST_WORKER"),
        SERVER_WORKER_BOOTED("WORKER_BOOTED"),
        SERVER_KILL_WORKER("KILL"),
        SERVER_DISPATCH_REQUEST("DISPATCH"),
        SERVER_RECEIVE_RESPONSE("RESPONSE"),
        SERVER_CACHE_HIT("CACHE_HIT"),
        SERVER_QUARANTINE("QUARANTINE"),
        SERVER_CLEAR_QUARANTINE("CLEAR_QUARANTINE"),

        WORKER_BOOT("START_WORKER"),
        WORKER_BOOT_THREAD("START_THREAD"),
        WORKER_BLOCK_THREAD("BLOCK_THREAD"),
        WORKER_UNBLOCK_THREAD("UNBLOCK_THREAD"),
        WORKER_KILL_WORKER("KILL_WORKER"),
        WORKER_RECEIVE_TASK("WORKER_RECEIVE"),
        WORKER_RETURN_TASK("WORKER_RETURN"),
        WORKER_REPORT_CPU_STATS("CPU_DUMP");

        protected final String eventTag;

        EventType(String eventTag) {
            this.eventTag = eventTag;
        }

        boolean matches (final String log) {
            return log.startsWith(eventTag + ":");
        }
    }

    public enum EventProperty {
        /** Worker ID as assigned by master*/
        WORKER_ID("worker_id"),
        /** Tag for some sort of communication*/
        TAG("tag"),
        /** Thread ID from a worker node*/
        TID("tid"),
        /** Which job type: one of [418wisdom, projectidea, tellmenow, countprimes, compareprimes]*/
        JOB_TYPE("job"),

        // CPU activity
        USER_CPU_JIFFIES("user_cpu_jiffies"),
        LOW_CPU_JIFFIES("low_cpu_jiffies"),
        SYS_CPU_JIFFIES("sys_cpu_jiffies"),
        IDLE_CPU_JIFFIES("idle_cpu_jiffies"),
        IO_CPU_JIFFIES("io_cpu_jiffies"),
        CPU_SPECIFIC_DATA("cpu_data"),

        // Request paramaters
        CMD("cmd");

        String propName;

        EventProperty(String propName) {
            this.propName = propName;
        }

        public static EventProperty toProp (final String str) {
            for (EventProperty p : values()) {
                if (p.propName.equals(str)) {
                    return p;
                }
            }
            throw new IllegalArgumentException(str + " is not a valid property name.");
        }
    }

    public enum JobType {
        JOB_418wisdom("418wisdom", 2500L),
        JOB_projectidea("projectidea", 4100L),
        JOB_tellmenow("tellmenow", 150L),
        JOB_countprimes("countprimes", 2000L),
        JOB_compareprimes("compareprimes", 2000L);

        private String jobname;
        private long latencyThreshold;

        JobType(String jobname, long latencyThreshold) {
            this.jobname = jobname;
            this.latencyThreshold = latencyThreshold;
        }

        public String toString () {
            return jobname;
        }

        public long getLatencyThreshold() {
            return latencyThreshold;
        }

        public static JobType parseType (final String s) {
            for (final JobType t : values()) {
                if (t.jobname.equals(s)) {
                    return t;
                }
            }
            throw new IllegalArgumentException("Error: string \"" + s + "\" is not a valid job type.");
        }
    }

    protected final EventType event;
    protected Date time;
    protected final HashMap<EventProperty, Object> eventProps;

    public LogEvent(EventType event, final Date time, final String message) {
        this.event = event;
        this.time = time;
        this.eventProps = new HashMap<>();
        extractProps(message);
    }

    protected void extractProps(final String message) {
        final String body = message.replace(event.eventTag + ":", "");
        final List<String> props = Arrays.asList(body.split(","));
        props.forEach(propString -> {
            final String[] propArr = propString.split(":");
            final EventProperty key = EventProperty.toProp(propArr[0].trim());
            final String valStr = propArr[1].trim();
            try {
                if (event == EventType.WORKER_REPORT_CPU_STATS) {
                    if (key == EventProperty.CPU_SPECIFIC_DATA) { // need to parse the CPU list
                        final TreeMap<Integer, CPUStats> cpuStats = new TreeMap<>();

                        // form is:<#=[...|...|...];#=[...|...|...];>, etc
                        final String noBrackets = valStr.substring(1, valStr.length() - 1);
                        final List<String> cpus = Arrays.asList(noBrackets.split(";"));
                        cpus.removeIf(String::isEmpty);
                        cpus.forEach(S -> {
                            final String[] idVals = S.split("=");
                            final int cpuID = Integer.parseInt(idVals[0]);
                            final String[] vals = idVals[1].substring(1, idVals[1].length()-1).split("\\|");

                            final CPUStats stats = new CPUStats(
                                    Integer.parseInt(vals[0]),
                                    Integer.parseInt(vals[1]),
                                    Integer.parseInt(vals[2]),
                                    Integer.parseInt(vals[3]),
                                    Integer.parseInt(vals[4])
                            );
                            cpuStats.put(cpuID, stats);
                        });

                        eventProps.put(key, cpuStats);
                    } else {
                        final long longVal = Long.parseLong(valStr);
                        eventProps.put(key, longVal);
                    }
                } else if (key == EventProperty.JOB_TYPE) {
                    final JobType jobType = JobType.parseType(valStr);
                    eventProps.put(key, jobType);
                } else if (key == EventProperty.CMD) {
                    eventProps.put(key, valStr);
                } else {
                    final int intVal = Integer.parseInt(valStr);
                    eventProps.put(key, intVal);
                }
            } catch (NumberFormatException ex) {
                eventProps.put(key, valStr);
            }
        });
    }

    public class CPUStats {
        protected long userJiffies;
        protected long lowJiffies;
        protected long systemJiffies;
        protected long idleJiffies;
        protected long IOJiffies;

        public CPUStats(long userJiffies, long lowJiffies, long systemJiffies, long idleJiffies, long IOJiffies) {
            this.userJiffies = userJiffies;
            this.lowJiffies = lowJiffies;
            this.systemJiffies = systemJiffies;
            this.idleJiffies = idleJiffies;
            this.IOJiffies = IOJiffies;
        }

        public long getUserJiffies() {
            return userJiffies;
        }

        public long getLowJiffies() {
            return lowJiffies;
        }

        public long getSystemJiffies() {
            return systemJiffies;
        }

        public long getIdleJiffies() {
            return idleJiffies;
        }

        public long getIOJiffies() {
            return IOJiffies;
        }

        @Override
        public String toString() {
            return "CPUStats{" +
                    "userJiffies=" + userJiffies +
                    ", lowJiffies=" + lowJiffies +
                    ", systemJiffies=" + systemJiffies +
                    ", idleJiffies=" + idleJiffies +
                    ", IOJiffies=" + IOJiffies +
                    '}';
        }
    }

    public void addToTime (final long delta) {
        this.time = new Date(this.time.getTime() + delta);
    }

    public boolean hasWorkerID () {
        return eventProps.containsKey(EventProperty.WORKER_ID);
    }

    public Optional<Integer> getWorkerID () {
        return getIntProp(EventProperty.WORKER_ID);
    }

    public boolean hasTag () {
        return eventProps.containsKey(EventProperty.TAG);
    }

    public Optional<Integer> getTag() {
        return getIntProp(EventProperty.TAG);
    }

    public Optional<Integer> getTID (){
        return getIntProp(EventProperty.TID);
    }

    protected Optional<Integer> getIntProp(final EventProperty p) {
        if (eventProps.containsKey(p)) {
            return Optional.of((int) eventProps.get(p));
        } else {
            return Optional.empty();
        }
    }

    public long getUserCPUJiffies () {
        return getLongProp(EventProperty.USER_CPU_JIFFIES).get();
    }

    public long getLowCPUJiffies () {
        return getLongProp(EventProperty.LOW_CPU_JIFFIES).get();
    }

    public long getSysCPUJiffies () {
        return getLongProp(EventProperty.SYS_CPU_JIFFIES).get();
    }

    public long getIdleCPUJiffies () {
        return getLongProp(EventProperty.IDLE_CPU_JIFFIES).get();
    }

    public long getIOCPUJiffies () {
        return getLongProp(EventProperty.IO_CPU_JIFFIES).get();
    }

    public SortedMap<Integer, CPUStats> getCPUStats () {
        return (SortedMap<Integer, CPUStats>) eventProps.get(EventProperty.CPU_SPECIFIC_DATA);
    }

    public int getCPUCount () {
        if (event != EventType.WORKER_REPORT_CPU_STATS) {
            throw new IllegalStateException("Can't count CPUs on any request but WORKER_REPORT_CPU_STATS");
        }

        return getCPUStats().size();
    }

    protected Optional<Long> getLongProp (final EventProperty p) {
        if (eventProps.containsKey(p)) {
            return Optional.of((long) eventProps.get(p));
        } else {
            return Optional.empty();
        }
    }

    public Optional<JobType> getJobType () {
        if (eventProps.containsKey(EventProperty.JOB_TYPE)) {
            return Optional.of((JobType) eventProps.get(EventProperty.JOB_TYPE));
        } else {
            return Optional.empty();
        }
    }

    public Optional<String> getJobCommand () {
        if (event != EventType.SERVER_DISPATCH_REQUEST) {
            return Optional.empty();
        } else {
            return Optional.of((String) eventProps.get(EventProperty.CMD));
        }
    }

    public long getTime () {
        return time.getTime();
    }

    public EventType getLogEventType () {
        return event;
    }

    public String toString () {
        return event.toString() + " | " + getTime();
    }
}
