package com.loadbalancers.analysis.analyzers.balancer;


import com.loadbalancers.analysis.analyzers.Analyzer;
import com.loadbalancers.analysis.events.LogEventStream;

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
