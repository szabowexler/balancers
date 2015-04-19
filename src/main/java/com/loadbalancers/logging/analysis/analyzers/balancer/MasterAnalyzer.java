package com.loadbalancers.logging.analysis.analyzers.balancer;


import com.loadbalancers.logging.analysis.analyzers.Analyzer;
import com.loadbalancers.logging.LogEventStream;

/**
 * @author Elias Szabo-Wexler
 * @since 22/March/2015
 */

public abstract class MasterAnalyzer extends Analyzer{

    public MasterAnalyzer() {
    }

    public MasterAnalyzer(boolean showLegend) {
        super(showLegend);
    }

    public abstract void analyze (final LogEventStream s);
}
