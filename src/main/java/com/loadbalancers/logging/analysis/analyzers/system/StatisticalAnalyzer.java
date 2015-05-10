package com.loadbalancers.logging.analysis.analyzers.system;

import com.loadbalancers.logging.LogEventStream;
import com.loadbalancers.logging.Logs;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jfree.chart.renderer.xy.XYSplineRenderer;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Elias Szabo-Wexler
 * @since 03/May/2015
 */
@Component
public class StatisticalAnalyzer extends GlobalAnalyzer {
    private final static Logger log = LogManager.getLogger(StatisticalAnalyzer.class);
    public final static long STATS_GRANULARITY_MS = 100;
    public final static String STATS_BASE_FILENAME = "StatsGraph.png";
    public final static String STATS_X_AXIS_LABEL = "Time (S)";

    @Override
    public void analyze(List<LogEventStream> workerStreams, LogEventStream masterStream) {
        System.out.println("Analyzing stats...");
        final long streamTime = masterStream.getStreamDurationMS();
        final List<LogEventStream> loadEvents =
                workerStreams.stream().map(S -> S.filterForType(Logs.LogEventType.WORKER_EVENT_REPORT_LOAD)).collect(Collectors.toList());

        final XYSeries meanSeries = new XYSeries("Mean");
        final XYSeries varianceSeries = new XYSeries("Variance");
        final XYSeries skewnessSeries = new XYSeries("Skewness");
        final XYSeries kurtosisSeries = new XYSeries("Kurtosis");

        makeDatasets(loadEvents, streamTime, meanSeries, varianceSeries, skewnessSeries, kurtosisSeries);

        makeGraph(meanSeries);
        makeGraph(varianceSeries);
        makeGraph(skewnessSeries);
        makeGraph(kurtosisSeries);

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
                .map(StatisticalAnalyzer::computeLoad)
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
                .map(StatisticalAnalyzer::computeLoad)
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

    protected void makeGraph (final XYSeries s) {
        final String prefix = (String) s.getKey();
        createLinePlotPNG(prefix, STATS_X_AXIS_LABEL, prefix,
                new XYSeriesCollection(s), SYSTEM_SUBFOLDER, prefix + STATS_BASE_FILENAME);
    }

    @Override
    protected void configureChartBeforeSave() {
        chart.getXYPlot().setRenderer(new XYSplineRenderer());
    }
}
