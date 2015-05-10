package com.loadbalancers.logging.analysis.analyzers;


import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartUtilities;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.title.LegendTitle;
import org.jfree.data.xy.TableXYDataset;
import org.jfree.data.xy.XYDataset;
import org.jfree.ui.RectangleEdge;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * @author Elias Szabo-Wexler
 * @since 22/March/2015
 */
public abstract class Analyzer {
    public static String ANALYSIS_FOLDER_NAME = "/home/elias/Desktop/418/HW/asst4/log_analysis";

    public static String BALANCER_SUBFOLDER_NAME = "balancer";
    public static String WORKER_SUBFOLDER_NAME = "workers";
    public static String SYSTEM_SUBFOLDER_NAME = "system";
    public static String ALL_SUBFOLDER_NAME = "all";

    public static File ANALYSIS_FOLDER = null;
    public static File BALANCER_SUBFOLDER = null;
    public static File WORKER_SUBFOLDER = null;
    public static File SYSTEM_SUBFOLDER = null;
    public static File ALL_SUBFOLDER = null;

    public static long DEFAULT_GRANULARITY_MS = 500;
    public static int GRAPH_WIDTH = 800;
    public static int GRAPH_HEIGHT = 400;

    protected boolean showLegend;
    protected JFreeChart chart;

    public Analyzer() {
        this(false);
    }

    public Analyzer(boolean showLegend) {
        this.showLegend = showLegend;
    }

    protected void createLinePlotPNG(final String title,
                                     final String xLabel,
                                     final String yLabel,
                                     final XYDataset dataset,
                                     final File folder,
                                     final String fileName) {
        chart = ChartFactory.createXYLineChart(
                title,
                xLabel,
                yLabel,
                dataset,
                PlotOrientation.VERTICAL,
                showLegend,
                true,
                false);

        configureChartBeforeSave();

        final LegendTitle legend = chart.getLegend();
        if (legend != null) {
            legend.setPosition(RectangleEdge.RIGHT);
        }

        try {
            final Path p = Paths.get(folder.getAbsolutePath(), fileName);
            final OutputStream os = Files.newOutputStream(p);
            ChartUtilities.writeChartAsPNG(os, chart, GRAPH_WIDTH, GRAPH_HEIGHT);

            final Path p2 = Paths.get(ALL_SUBFOLDER.getAbsolutePath(), fileName);
            final OutputStream os2 = Files.newOutputStream(p2);
            ChartUtilities.writeChartAsPNG(os2, chart, GRAPH_WIDTH, GRAPH_HEIGHT);
        } catch (IOException ex) {
            ex.printStackTrace();
            throw new RuntimeException("Unable to write " + fileName + " out:\t" + ex);
        }
    }

    protected void configureChartBeforeSave () {

    }

    protected void createStackedAreaChart(final String title,
                                     final String xLabel,
                                     final String yLabel,
                                     final TableXYDataset dataset,
                                     final File folder,
                                     final String fileName) {
        chart = ChartFactory.createStackedXYAreaChart(
                title,
                xLabel,
                yLabel,
                dataset,
                PlotOrientation.VERTICAL,
                showLegend,
                true,
                false);

        final LegendTitle legend = chart.getLegend();
        if (legend != null) {
            legend.setPosition(RectangleEdge.RIGHT);
        }

        try {
            final Path p = Paths.get(folder.getAbsolutePath(), fileName);
            final OutputStream os = Files.newOutputStream(p);
            ChartUtilities.writeChartAsPNG(os, chart, GRAPH_WIDTH, GRAPH_HEIGHT);

            final Path p2 = Paths.get(ALL_SUBFOLDER.getAbsolutePath(), fileName);
            final OutputStream os2 = Files.newOutputStream(p2);
            ChartUtilities.writeChartAsPNG(os2, chart, GRAPH_WIDTH, GRAPH_HEIGHT);
        } catch (IOException ex) {
            ex.printStackTrace();
            throw new RuntimeException("Unable to write " + fileName + " out:\t" + ex);
        }
    }

    public static void initializeAnalysis(final File analysisFolder) {
        ANALYSIS_FOLDER = analysisFolder;

        if (!analysisFolder.exists()) {
            analysisFolder.mkdir();
        }

        BALANCER_SUBFOLDER = Paths.get(analysisFolder.getAbsolutePath(), BALANCER_SUBFOLDER_NAME).toFile();
        if (!BALANCER_SUBFOLDER.exists()) {
            if (!BALANCER_SUBFOLDER.mkdir()) {
                throw new RuntimeException("Unable to create balancer subfolder.");
            }
        }

        WORKER_SUBFOLDER = Paths.get(analysisFolder.getAbsolutePath(), WORKER_SUBFOLDER_NAME).toFile();
        if (!WORKER_SUBFOLDER.exists()) {
            if (!WORKER_SUBFOLDER.mkdir()) {
                throw new RuntimeException("Unable to create worker subfolder.");
            }
        }

        SYSTEM_SUBFOLDER = Paths.get(analysisFolder.getAbsolutePath(), SYSTEM_SUBFOLDER_NAME).toFile();
        if (!SYSTEM_SUBFOLDER.exists()) {
            if (!SYSTEM_SUBFOLDER.mkdir()) {
                throw new RuntimeException("Unable to create system subfolder.");
            }
        }

        ALL_SUBFOLDER = Paths.get(analysisFolder.getAbsolutePath(), ALL_SUBFOLDER_NAME).toFile();
        if (!ALL_SUBFOLDER.exists()) {
            if (!ALL_SUBFOLDER.mkdir()) {
                throw new RuntimeException("Unable to create all subfolder.");
            }
        }
    }
}
