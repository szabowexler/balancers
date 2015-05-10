package com.loadbalancers.logging.analysis.analyzers.comparison;

import com.loadbalancers.logging.analysis.Run;
import com.loadbalancers.logging.analysis.analyzers.Analyzer;

import java.util.List;
import java.util.Map;

/**
 * @author Elias Szabo-Wexler
 * @since 08/May/2015
 */
public abstract class ComparisonAnalyzer extends Analyzer {

    public ComparisonAnalyzer(boolean showLegend) {
        super(showLegend);
    }

    public abstract void analyze (final Map<String, List<Run>> runs);
}
