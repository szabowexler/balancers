package com.loadbalancers.logging.analysis;


import com.loadbalancers.logging.EventLogger;
import com.loadbalancers.logging.LogEventStream;
import com.loadbalancers.logging.Logs;
import com.loadbalancers.logging.analysis.analyzers.Analyzer;
import com.loadbalancers.logging.analysis.analyzers.balancer.MasterAnalyzer;
import com.loadbalancers.logging.analysis.analyzers.comparison.ComparisonAnalyzer;
import com.loadbalancers.logging.analysis.analyzers.system.GlobalAnalyzer;
import com.loadbalancers.logging.analysis.analyzers.workers.WorkersAnalyzer;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.swing.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Elias Szabo-Wexler
 * @since 20/March/2015
 */
public class AnalysisRunner {
    public static void main (final String[] args) throws IOException {
        ApplicationContext context = new ClassPathXmlApplicationContext("spring-config.xml");

        final File logDir;
        if (args.length == 0) {

            JFileChooser fc = new JFileChooser();
            fc.setFileSelectionMode(JFileChooser.FILES_AND_DIRECTORIES);
            fc.setFileHidingEnabled(false);
            fc.setCurrentDirectory(new File(".."));
            fc.showDialog(null, "Select");

            logDir = fc.getSelectedFile();
        } else {
            logDir = new File(args[0]);
        }

        final Map<String, List<Run>> runs = new ConcurrentHashMap<>();
        for (final File f : logDir.listFiles()) {
            final String fName = f.getName();
            if (fName.endsWith("logevents.log")) {
                final String prefix = fName.replaceAll("\\.logevents\\.log", "");
                final String runName = prefix.replaceAll("\\..*", "");
                if (!runs.containsKey(runName)) {
                    runs.put(runName, new ArrayList<>());
                }
                System.out.println("============ Analyzing " + prefix + " ==============");
                final File traceAnalysisDir = Paths.get(logDir.getPath(), prefix).toFile();
                Analyzer.initializeAnalysis(traceAnalysisDir);

                final LogEventStream stream = loadLogEventStream(f);
                final LogEventStream masterStream = stream.extractMasterStream();
                final List<LogEventStream> workerStreams = stream.extractWorkerStreams();
//                analyzeMasterStream(masterStream, context);
//                analyzeWorkerStreams(workerStreams, context);
//                analyzeGlobalSystem(workerStreams, masterStream, context);
                runs.get(runName).add(new Run(masterStream, workerStreams));
            }
        }

        Analyzer.initializeAnalysis(Paths.get(logDir.getPath()).toFile());
        compareRuns(context, runs);
    }

    protected static LogEventStream loadLogEventStream (final File f) throws IOException {
        final List<Logs.LogEvent> logEvents = EventLogger.readEventLog(f);
        final LogEventStream stream = new LogEventStream(logEvents);
        if (stream.size() == 0) {
            System.out.println("Loaded empty stream. Quitting.");
            System.exit(0);
        }
        stream.setStreamStart(0);
        System.out.println("Loaded stream: " + stream);
        return stream;
    }


    protected static void analyzeMasterStream (final LogEventStream stream, final ApplicationContext context) throws IOException{
        // Do analysis on master stream
        System.out.println("--- Analyzing master stream.");
        System.out.println("===============================================================");
        Map<String, MasterAnalyzer> analyzerBeans = context.getBeansOfType(MasterAnalyzer.class);
        for (final MasterAnalyzer analyzer : analyzerBeans.values()) {
            analyzer.analyze(stream);
        }
        System.out.println("===============================================================");
    }

    protected static void analyzeWorkerStreams (final List<LogEventStream> workerStreams, final ApplicationContext context) throws IOException{
        // Do analysis on worker streams
        System.out.println("--- Analyzing worker streams.");
        System.out.println("===============================================================");
        Map<String, WorkersAnalyzer> analyzerBeans = context.getBeansOfType(WorkersAnalyzer.class);
        for (final WorkersAnalyzer analyzer : analyzerBeans.values()) {
            analyzer.analyze(workerStreams);
        }
        System.out.println("===============================================================");
    }

    protected static void analyzeGlobalSystem (final List<LogEventStream> workerStreams,
                                               final LogEventStream masterStream,
                                               final ApplicationContext context) throws IOException{
        // Do analysis on everything
        System.out.println("--- Analyzing joint streams.");
        System.out.println("===============================================================");
        Map<String, GlobalAnalyzer> analyzerBeans = context.getBeansOfType(GlobalAnalyzer.class);
        for (final GlobalAnalyzer analyzer : analyzerBeans.values()) {
            analyzer.analyze(workerStreams, masterStream);
        }
        System.out.println("===============================================================");
    }

    protected static void compareRuns (final ApplicationContext context,
                                       final Map<String, List<Run>> runs) {
        // Do analysis on everything
        System.out.println("--- Comparing runs.");
        System.out.println("===============================================================");
        Map<String, ComparisonAnalyzer> analyzerBeans = context.getBeansOfType(ComparisonAnalyzer.class);
        for (final ComparisonAnalyzer analyzer : analyzerBeans.values()) {
            analyzer.analyze(runs);
        }
        System.out.println("===============================================================");
    }
}
