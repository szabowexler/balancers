package com.loadbalancers.logging.analysis;


import com.loadbalancers.logging.Logs;
import com.loadbalancers.logging.analysis.analyzers.Analyzer;
import com.loadbalancers.logging.analysis.analyzers.balancer.MasterAnalyzer;
import com.loadbalancers.logging.analysis.analyzers.system.GlobalAnalyzer;
import com.loadbalancers.logging.analysis.analyzers.workers.WorkersAnalyzer;
import com.loadbalancers.logging.LogEventStream;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import javax.swing.*;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author Elias Szabo-Wexler
 * @since 20/March/2015
 */
public class Runner {
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
        Analyzer.initializeAnalysis(logDir);
        final LogEventStream masterStream = loadMasterStream(logDir);
        final List<LogEventStream> workerStreams = loadWorkerStreams(masterStream, logDir);
        analyzeMasterStream(masterStream, context);
        analyzeWorkerStreams(workerStreams, context);
        analyzeGlobalSystem(workerStreams, masterStream, context);
    }

    protected static LogEventStream loadMasterStream (final File logDir) {
        final ArrayList<String> masterLogLines = new ArrayList<>();
        Arrays.asList(logDir.listFiles()).forEach(f -> {
            if (f.getName().startsWith("master.") && !f.getName().endsWith(".INFO")) {
                try {
                    System.out.println("Reading master log:\t" + f);
                    masterLogLines.addAll(Files.readAllLines(f.toPath()));
                } catch (IOException ex) {
                    System.err.println("Unable to read file:\t" + f);
                    ex.printStackTrace();
                }
            }
        });

        final LogEventStream stream = new LogEventStream();
        stream.parse(masterLogLines);
        if (stream.size() == 0) {
            System.out.println("Loaded empty master stream. Quitting.");
            System.exit(0);
        }
        stream.setStreamStart(0);
        System.out.println("Loaded master stream: " + stream);
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

    protected static List<LogEventStream> loadWorkerStreams (final LogEventStream masterStream, final File logDir) {
        final ArrayList<LogEventStream> workerStreams = new ArrayList<>();
        Arrays.asList(logDir.listFiles()).forEach(f -> {
            if (f.getName().startsWith("worker.") && !f.getName().endsWith(".INFO")) {
                try {
                    System.out.println("Reading worker log:\t" + f);
                    final List<String> workerLogLines = Files.readAllLines(f.toPath());
                    final LogEventStream stream = new LogEventStream();
                    stream.parse(workerLogLines);

                    // Now adjust the stream to match the master stream
                    final int workerID = stream.getWorkerID().get();
                    final LogEventStream bootStream = masterStream.filterForType(Logs.LogEventType.SERVER_EVENT_WORKER_BOOTED);
                    final long streamStart = bootStream.getEventsForWorker(workerID).get(0).getTime();
                    System.out.println("Adjusting stream [worker " + workerID + "] to start at:\t" + streamStart);
                    stream.setStreamStart(streamStart);
                    workerStreams.add(stream);
                } catch (IOException ex) {
                    System.err.println("Unable to read file:\t" + f);
                    ex.printStackTrace();
                }
            }
        });
        System.out.println("Loaded worker streams:");
        workerStreams.forEach(s -> System.out.println("\t" + s));
        return workerStreams;
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

    protected static void analyzeGlobalSystem (final List<LogEventStream> workerStreams, final LogEventStream masterStream, final ApplicationContext context) throws IOException{
        // Do analysis on everything
        System.out.println("--- Analyzing joint streams.");
        System.out.println("===============================================================");
        Map<String, GlobalAnalyzer> analyzerBeans = context.getBeansOfType(GlobalAnalyzer.class);
        for (final GlobalAnalyzer analyzer : analyzerBeans.values()) {
            analyzer.analyze(workerStreams, masterStream);
        }
        System.out.println("===============================================================");
    }
}
