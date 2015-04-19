package com.loadbalancers.logging.analysis.analyzers.workers;


import com.loadbalancers.logging.analysis.analyzers.system.NetworkLatencyAnalyzer;
import com.loadbalancers.logging.analysis.events.LogEvent;
import com.loadbalancers.logging.LogEventStream;
import org.jfree.chart.axis.NumberAxis;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Elias Szabo-Wexler
 * @since 23/March/2015
 */

@Component
public class WorkerCPUEfficiencyAnalyzer extends WorkersAnalyzer {
    public final static String WORKER_CPU_USER_GRAPH_FILENAME = "WorkerUserCPUEfficiencyGraph.png";
    public final static String WORKER_CPU_USER_GRAPH_TITLE = "Worker CPU Efficiency";
    public final static String WORKER_CPU_USER_X_AXIS_LABEL = "Time (S)";
    public final static String WORKER_CPU_USER_Y_AXIS_LABEL = "% Time CPU Utilized";

    public final static String WORKER_IDLE_CPU_EFFICIENCY_GRAPH_FILENAME = "WorkerIdleCPUGraph.png";
    public final static String WORKER_IDLE_CPU_EFFICIENCY_GRAPH_TITLE = "Worker CPU Idle";
    public final static String WORKER_IDLE_CPU_EFFICIENCY_X_AXIS_LABEL = "Time (S)";
    public final static String WORKER_IDLE_CPU_EFFICIENCY_Y_AXIS_LABEL = "% Time CPU Idle";

    public final static String WORKER_IO_CPU_EFFICIENCY_GRAPH_FILENAME = "WorkerIOCPUGraph.png";
    public final static String WORKER_IO_CPU_EFFICIENCY_GRAPH_TITLE = "Worker CPU I/O";
    public final static String WORKER_IO_CPU_EFFICIENCY_X_AXIS_LABEL = "Time (S)";
    public final static String WORKER_IO_CPU_EFFICIENCY_Y_AXIS_LABEL = "% Time CPU Waiting on I/O";

    public final static String WORKER_IND_CPU_EFF_GRAPH_TITLE = "Worker CPU Efficiency (By Core)";
    public final static String WORKER_IND_CPU_EFF_X_AXIS_LABEL = "Time (S)";
    public final static String WORKER_IND_CPU_EFF_Y_AXIS_LABEL = "% Time CPU Utilized";

    public final static String WORKER_IND_CPU_DOWN_GRAPH_TITLE = "Number Unutilized CPUs";
    public final static String WORKER_IND_CPU_DOWN_X_AXIS_LABEL = "Time (S)";
    public final static String WORKER_IND_CPU_DOWN_Y_AXIS_LABEL = "Number CPUs Unutilized";

    public static String getWorkerIndCPUEffGraphFilename (int w) {
        return "Worker" + w + "UserCPUEfficiencyGraph.png";
    }

    public static String getWorkerIndCPUDownGraphFilename (int w) {
        return "Worker" + w + "UserCPUDownGraph.png";
    }

    public WorkerCPUEfficiencyAnalyzer() {
        super(true);
    }

    @Override
    public void analyze(final List<LogEventStream> s) {
        System.out.println("Analyzing worker CPU efficiency...");

        createUserCPUGraph(s);
        createPerCoreUserCPUGraphs(s);
        createPerCoreCPUDownGraphs(s);

        createIdleCPUGraph(s);
        // These don't seem to be super useful, so leave them out for now
//        createIOCPUGraph(s);

        System.out.println("Worker CPU efficiency analysis completed.");
    }

    protected void createUserCPUGraph (final List<LogEventStream> s) {
        final List<XYSeries> series = createUserDatasets(s);
        final XYSeriesCollection dataset = new XYSeriesCollection();
        series.forEach(dataset::addSeries);

        createLinePlotPNG(WORKER_CPU_USER_GRAPH_TITLE, WORKER_CPU_USER_X_AXIS_LABEL, WORKER_CPU_USER_Y_AXIS_LABEL,
                dataset, WORKER_SUBFOLDER, WORKER_CPU_USER_GRAPH_FILENAME);
        XYPlot plot = chart.getXYPlot();
        final NumberAxis axis = (NumberAxis) plot.getRangeAxis();
        axis.setAutoRange(false);
        axis.setRange(0, 100);
    }

    protected List<XYSeries> createUserDatasets(final List<LogEventStream> s) {
        ArrayList<XYSeries> datasets = new ArrayList<>();
        final Map<Integer, LogEventStream> workerIdToLogStream = NetworkLatencyAnalyzer.makeWorkerToStreamMap(s);
        workerIdToLogStream.forEach((workerID, stream) -> datasets.add(createUserCPUData(workerID, stream)));

        return datasets;
    }


    protected XYSeries createUserCPUData(int i, final LogEventStream s) {
        final XYSeries series = new XYSeries("W" + i);

        final LogEventStream cpuStream = s.filterForType(LogEvent.EventType.WORKER_REPORT_CPU_STATS);
        final ListIterator<LogEvent> eventIterator = cpuStream.iterator();
        if (eventIterator.hasNext()) {
            LogEvent prev = eventIterator.next();

            while (eventIterator.hasNext()) {
                final LogEvent curr = eventIterator.next();

                double time = (double) curr.getTime() / 1000.;
                series.add(time, getUserCPUUsage(prev, curr));

                prev = curr;
            }
        }

        return series;
    }

    protected double getUserCPUUsage (final LogEvent start, final LogEvent end) {
        final double jiffies = getTotalJiffies(start, end);
        final double userJiffies = end.getUserCPUJiffies() - start.getUserCPUJiffies();
        final double perc = userJiffies / jiffies * 100.;
        return perc;
    }

    protected double getTotalJiffies (final LogEvent start, final LogEvent end) {
        return end.getUserCPUJiffies() - start.getUserCPUJiffies() +
                end.getLowCPUJiffies() - start.getLowCPUJiffies() +
                end.getSysCPUJiffies() - start.getSysCPUJiffies() +
                end.getIdleCPUJiffies() - start.getIdleCPUJiffies() +
                end.getIOCPUJiffies() - start.getIOCPUJiffies();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    protected void createPerCoreUserCPUGraphs (final List<LogEventStream> L) {
        L.forEach(this::createPerCoreUserCPUGraph);
    }

    protected void createPerCoreUserCPUGraph(LogEventStream s) {
        final Collection<XYSeries> series = createPerCoreUserDatasets(s);
        final XYSeriesCollection dataset = new XYSeriesCollection();
        series.forEach(dataset::addSeries);

        final String WORKER_IND_CPU_EFF_GRAPH_FILENAME = getWorkerIndCPUEffGraphFilename(s.getWorkerID().get());
        createLinePlotPNG(WORKER_IND_CPU_EFF_GRAPH_TITLE, WORKER_IND_CPU_EFF_X_AXIS_LABEL, WORKER_IND_CPU_EFF_Y_AXIS_LABEL,
                dataset, WORKER_SUBFOLDER, WORKER_IND_CPU_EFF_GRAPH_FILENAME);
        XYPlot plot = chart.getXYPlot();
        final NumberAxis axis = (NumberAxis) plot.getRangeAxis();
        axis.setAutoRange(false);
        axis.setAutoRangeIncludesZero(false);
        axis.setAutoRangeStickyZero(false);
        axis.setRange(-5, 105);
        axis.setRangeWithMargins(-2, 102);
    }

    protected Collection<XYSeries> createPerCoreUserDatasets(final LogEventStream s) {
        final ConcurrentHashMap<Integer, XYSeries> datasets = new ConcurrentHashMap<>();

        final LogEventStream cpuStream = s.filterForType(LogEvent.EventType.WORKER_REPORT_CPU_STATS);
        final ListIterator<LogEvent> eventIterator = cpuStream.iterator();
        if (eventIterator.hasNext()) {
            LogEvent prev = eventIterator.next();
            final int cpuCount = prev.getCPUCount();
            for (int i = 0; i < cpuCount; i++) {
                datasets.put(i, new XYSeries("C" + i));
            }

            while (eventIterator.hasNext()) {
                final LogEvent curr = eventIterator.next();

                double time = (double) curr.getTime() / 1000.;
                for (int i = 0; i < cpuCount; i ++) {
                    final XYSeries series = datasets.get(i);
                    series.add(time, getCoreUserCPUUsage(i, prev, curr));
                }

                prev = curr;
            }
        }

        return datasets.values();
    }

    protected double getCoreUserCPUUsage (final int c, final LogEvent start, final LogEvent end) {
        final LogEvent.CPUStats startStats = start.getCPUStats().get(c);
        final LogEvent.CPUStats endStats = end.getCPUStats().get(c);

        final double jiffies = getCoreTotalJiffies(startStats, endStats);
        final double userJiffies = endStats.getUserJiffies() - startStats.getUserJiffies();
        final double perc = userJiffies / jiffies * 100.;
        return perc;
    }

    protected double getCoreTotalJiffies (final LogEvent.CPUStats startStats, final LogEvent.CPUStats endStats) {
        return endStats.getUserJiffies() - startStats.getUserJiffies() +
                endStats.getLowJiffies() - startStats.getLowJiffies() +
                endStats.getSystemJiffies() - startStats.getSystemJiffies() +
                endStats.getIdleJiffies() - startStats.getIdleJiffies() +
                endStats.getIOJiffies() - startStats.getIOJiffies();
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    protected void createPerCoreCPUDownGraphs (final List<LogEventStream> L) {
        L.forEach(this::createPerCoreCPUDownGraph);
    }

    protected void createPerCoreCPUDownGraph(LogEventStream s) {
        final XYSeries series = createPerCoreCPUDownDataset(s);
        final XYSeriesCollection dataset = new XYSeriesCollection();
        dataset.addSeries(series);

        final String WORKER_IND_CPU_DOWN_GRAPH_FILENAME = getWorkerIndCPUDownGraphFilename(s.getWorkerID().get());
        createLinePlotPNG(WORKER_IND_CPU_DOWN_GRAPH_TITLE, WORKER_IND_CPU_DOWN_X_AXIS_LABEL, WORKER_IND_CPU_DOWN_Y_AXIS_LABEL,
                dataset, WORKER_SUBFOLDER, WORKER_IND_CPU_DOWN_GRAPH_FILENAME);
    }

    protected XYSeries createPerCoreCPUDownDataset(final LogEventStream s) {
        final XYSeries data = new XYSeries("Unutilized CPU Count");

        final LogEventStream cpuStream = s.filterForType(LogEvent.EventType.WORKER_REPORT_CPU_STATS);
        final ListIterator<LogEvent> eventIterator = cpuStream.iterator();
        if (eventIterator.hasNext()) {
            LogEvent prev = eventIterator.next();
            final int cpuCount = prev.getCPUCount();

            while (eventIterator.hasNext()) {
                final LogEvent curr = eventIterator.next();

                double time = (double) curr.getTime() / 1000.;
                int numSpinning = 0;
                HashSet<Integer> spinningCores = new HashSet<>();
                for (int i = 0; i < cpuCount; i ++) {
                    final double percCPUUsed = getCoreUserCPUUsage(i, prev, curr);
                    if(percCPUUsed < 0.05) {
                        spinningCores.add(i);
                        numSpinning ++;
                    }
                }

                System.out.println("Detected " + spinningCores + " spinning at:\t" + time + "s.");
                data.add(time, numSpinning);
                prev = curr;
            }
        }

        return data;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    protected void createIdleCPUGraph (final List<LogEventStream> s) {
        final List<XYSeries> series = createIdleDatasets(s);
        final XYSeriesCollection dataset = new XYSeriesCollection();
        series.forEach(dataset::addSeries);

        createLinePlotPNG(WORKER_IDLE_CPU_EFFICIENCY_GRAPH_TITLE, WORKER_IDLE_CPU_EFFICIENCY_X_AXIS_LABEL, WORKER_IDLE_CPU_EFFICIENCY_Y_AXIS_LABEL,
                dataset, WORKER_SUBFOLDER, WORKER_IDLE_CPU_EFFICIENCY_GRAPH_FILENAME);
        XYPlot plot = chart.getXYPlot();
        final NumberAxis axis = (NumberAxis) plot.getRangeAxis();
        axis.setAutoRange(false);
        axis.setAutoRangeIncludesZero(false);
        axis.setAutoRangeStickyZero(false);
        axis.setRange(-5, 105);
    }


    protected List<XYSeries> createIdleDatasets(final List<LogEventStream> s) {
        ArrayList<XYSeries> datasets = new ArrayList<>();
        final Map<Integer, LogEventStream> workerIdToLogStream = NetworkLatencyAnalyzer.makeWorkerToStreamMap(s);
        workerIdToLogStream.forEach((workerID, stream) -> datasets.add(createIdleDataset(workerID, stream)));

        return datasets;
    }

    protected XYSeries createIdleDataset(int i, final LogEventStream s) {
        final XYSeries series = new XYSeries("W" + i);

        final LogEventStream cpuStream = s.filterForType(LogEvent.EventType.WORKER_REPORT_CPU_STATS);
        final ListIterator<LogEvent> eventIterator = cpuStream.iterator();
        if (eventIterator.hasNext()) {
            LogEvent prev = eventIterator.next();

            while (eventIterator.hasNext()) {
                final LogEvent curr = eventIterator.next();

                double time = (double) curr.getTime() / 1000.;
                series.add(time, getIdleCPUUsage(prev, curr));

                prev = curr;
            }
        }

        return series;
    }


    protected double getIdleCPUUsage (final LogEvent start, final LogEvent end) {
        final double jiffies = getTotalJiffies(start, end);
        final double idleJiffies = end.getIdleCPUJiffies() - start.getIdleCPUJiffies();
        final double perc = idleJiffies / jiffies * 100.;
        return perc;
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    protected void createIOCPUGraph (final List<LogEventStream> s) {
        final List<XYSeries> series = createIODatasets(s);
        final XYSeriesCollection dataset = new XYSeriesCollection();
        series.forEach(dataset::addSeries);

        createLinePlotPNG(WORKER_IO_CPU_EFFICIENCY_GRAPH_TITLE, WORKER_IO_CPU_EFFICIENCY_X_AXIS_LABEL, WORKER_IO_CPU_EFFICIENCY_Y_AXIS_LABEL,
                dataset, WORKER_SUBFOLDER, WORKER_IO_CPU_EFFICIENCY_GRAPH_FILENAME);
    }


    protected List<XYSeries> createIODatasets(final List<LogEventStream> s) {
        ArrayList<XYSeries> datasets = new ArrayList<>();
        final Map<Integer, LogEventStream> workerIdToLogStream = NetworkLatencyAnalyzer.makeWorkerToStreamMap(s);
        workerIdToLogStream.forEach((workerID, stream) -> datasets.add(createIODataset(workerID, stream)));

        return datasets;
    }

    protected XYSeries createIODataset(int i, final LogEventStream s) {
        final XYSeries series = new XYSeries("W" + i);

        final LogEventStream cpuStream = s.filterForType(LogEvent.EventType.WORKER_REPORT_CPU_STATS);
        final ListIterator<LogEvent> eventIterator = cpuStream.iterator();
        LogEvent prev = eventIterator.next();

        while (eventIterator.hasNext()) {
            final LogEvent curr = eventIterator.next();

            double time = (double) curr.getTime();
            series.add(time, getIOCPUUsage(prev, curr));

            prev = curr;
        }

        return series;
    }

    protected double getIOCPUUsage (final LogEvent start, final LogEvent end) {
        final double jiffies = getTotalJiffies(start, end);
        return (end.getIOCPUJiffies() - start.getIOCPUJiffies()) / jiffies * 100.;
    }
}
