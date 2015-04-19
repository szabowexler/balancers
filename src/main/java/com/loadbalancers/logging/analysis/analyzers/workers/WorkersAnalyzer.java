package com.loadbalancers.logging.analysis.analyzers.workers;


import com.loadbalancers.logging.analysis.analyzers.Analyzer;
import com.loadbalancers.logging.LogEventStream;

import java.util.List;

/**
 * @author Elias Szabo-Wexler
 * @since 22/March/2015
 */
public abstract class WorkersAnalyzer extends Analyzer {

    public WorkersAnalyzer() {
        super();
    }

    public WorkersAnalyzer(boolean showLegend) {
        super(showLegend);
    }

    public abstract void analyze (final List<LogEventStream> workerStreams);
}
