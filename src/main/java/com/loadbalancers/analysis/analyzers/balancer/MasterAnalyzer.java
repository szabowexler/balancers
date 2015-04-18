package com.loadbalancers.analysis.analyzers.balancer;

import main.analyzers.Analyzer;
import main.events.LogEventStream;

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
