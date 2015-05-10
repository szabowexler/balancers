package com.loadbalancers.logging.analysis.analyzers.comparison;

import com.loadbalancers.logging.LogEventStream;
import com.loadbalancers.logging.Logs;
import com.loadbalancers.logging.analysis.Run;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Elias Szabo-Wexler
 * @since 03/May/2015
 */
@Component
public class ComparisonStatisticalAnalyzer extends ComparisonAnalyzer {
    private final static Logger log = LogManager.getLogger(ComparisonStatisticalAnalyzer.class);
    public final static long STATS_GRANULARITY_MS = 100;
    public final static String STATS_BASE_FILENAME = "ComparisonStatsGraph.png";
    public final static String STATS_X_AXIS_LABEL = "Time (S)";

    public ComparisonStatisticalAnalyzer() {
        super(true);
    }

    @Override
    public void analyze(final Map<String, List<Run>> runs) {
        System.out.println("Comparing runs for stats...");

        for (final String runName : runs.keySet()) {
            System.out.println("Analyzing:\t" + runName);
            final XYSeriesCollection meanData = new XYSeriesCollection();
            final XYSeriesCollection varianceData = new XYSeriesCollection();
            final XYSeriesCollection skewnessData = new XYSeriesCollection();
            final XYSeriesCollection kurtosisData = new XYSeriesCollection();

            for (final Run r : runs.get(runName)) {
                final long duration = r.getMasterStream().getStreamDurationMS();
                final List<LogEventStream> workerStreams = r.getWorkerStreams();
                final List<LogEventStream> loadEvents =
                        workerStreams.stream().map(S -> S.filterForType(Logs.LogEventType.WORKER_EVENT_REPORT_LOAD)).collect(Collectors.toList());

                final String balancerTypeName = r.getBalancerType();
                final XYSeries meanSeries = new XYSeries(balancerTypeName);
                final XYSeries varianceSeries = new XYSeries(balancerTypeName);
                final XYSeries skewnessSeries = new XYSeries(balancerTypeName);
                final XYSeries kurtosisSeries = new XYSeries(balancerTypeName);

                makeDatasets(loadEvents, duration, meanSeries, varianceSeries, skewnessSeries, kurtosisSeries);

                meanData.addSeries(meanSeries);
                varianceData.addSeries(varianceSeries);
                skewnessData.addSeries(skewnessSeries);
                kurtosisData.addSeries(kurtosisSeries);
            }

            makeGraph(runName + " Mean", meanData);
            makeGraph(runName + " Variance", varianceData);
            makeGraph(runName + " Skewness", skewnessData);
            makeGraph(runName + " Kurtosis", kurtosisData);
        }

        System.out.println("Stats analysis completed.");
    }

    protected void makeDatasets (final List<LogEventStream> workerStreams,
                                 final long duration,
                                 final XYSeries meanSeries,
                                 final XYSeries varianceSeries,
                                 final XYSeries skewnessSeries,
                                 final XYSeries kurtosisSeries) {
        long time = 0;
        while (time < duration) {
            final long t  = time;
            final List<Logs.LogEvent> eventSet = workerStreams.stream()
                    .map(S -> S.getClosestEvent(t))
                    .collect(Collectors.toList());

            double mean = computeMean(eventSet);
            double variance = computeVariance(eventSet);
            double skewness = computeSkewness(eventSet);
            double kurtosis = computeKurtosis(eventSet);

            meanSeries.add(time / 1000.0, mean);
            varianceSeries.add(time / 1000.0, variance);
            skewnessSeries.add(time / 1000.0, skewness);
            kurtosisSeries.add(time / 1000.0, kurtosis);

            time += STATS_GRANULARITY_MS;
        }
    }

    protected static Double computeLoad (final Logs.LoadSnapshot snapshot) {
        return (double) (snapshot.getWorkerCurrentJobs() + snapshot.getWorkerQueueSize()) / (double) snapshot.getWorkerMaxConcurrentJobs();
    }

    protected double computeMean (final List<Logs.LogEvent> events) {
        return events.stream()
                .map(Logs.LogEvent::getLoad)
                .map(ComparisonStatisticalAnalyzer::computeLoad)
                .collect(Collectors.averagingDouble(d -> d));
    }


    protected double computeVariance (final List<Logs.LogEvent> events) {
        final double mean = computeMean(events);
        final int n = events.size();
        final double sum = events.stream()
                .map(Logs.LogEvent::getLoad)
                .map(l -> Math.pow(computeLoad(l) - mean, 2.0))
                .collect(Collectors.summingDouble(d -> d));
        return 1.0 / n * sum;
    }

    protected double computeSkewness (final List<Logs.LogEvent> events) {
        final double mean = computeMean(events);
        final double variance = computeVariance(events);
        final List<Double> loads = events.stream()
                .map(Logs.LogEvent::getLoad)
                .map(ComparisonStatisticalAnalyzer::computeLoad)
                .collect(Collectors.toList());
        final List<Double> mapped = loads.stream().map(l -> Math.pow(l - mean, 3.0)).collect(Collectors.toList());
        final double sum = mapped.stream().collect(Collectors.summingDouble(d -> d));
        final double numerator = 1 / mean * sum;
        final double denomenator = Math.pow(variance, 1.5);
        return numerator / denomenator;
    }

    protected double computeKurtosis (final List<Logs.LogEvent> events) {
        final double mean = computeMean(events);
        final double variance = computeVariance(events);
        final double numerator = 1 / mean * events.stream()
                .map(Logs.LogEvent::getLoad)
                .map(l -> Math.pow(computeLoad(l) - mean, 4.0))
                .collect(Collectors.summingDouble(d -> d));
        final double denomenator = Math.pow(variance, 2.0);
        return numerator / denomenator - 3;
    }

    protected void makeGraph (final String title, final XYSeriesCollection c) {
        createLinePlotPNG(title, STATS_X_AXIS_LABEL, title,
                c, SYSTEM_SUBFOLDER, title + STATS_BASE_FILENAME);
    }

    @Override
    protected void configureChartBeforeSave() {
//        chart.getXYPlot().setRenderer(new XYSplineRenderer());
    }
}
