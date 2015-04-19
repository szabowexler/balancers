package com.loadbalancers.logging.analysis.analyzers.system;

import com.loadbalancers.logging.analysis.analyzers.Analyzer;
import com.loadbalancers.logging.LogEventStream;

import java.util.List;

/**
 * @author Elias Szabo-Wexler
 * @since 23/March/2015
 */
public abstract class GlobalAnalyzer extends Analyzer{
    public abstract void analyze (final List<LogEventStream> workerStreams, final LogEventStream masterStream);
}
