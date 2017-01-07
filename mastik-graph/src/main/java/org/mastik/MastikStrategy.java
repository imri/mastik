package org.mastik;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.mastik.structure.MastikGraph;

/**
 * A wrapper for {@link Strategy} that implements {@link TraversalStrategy}
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since 1/6/17
 */
public class MastikStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    private Strategy strategy;

    public MastikStrategy(Strategy strategy) {
        this.strategy = strategy;
    }

    /**
     * Applies the strategy to a traversal
     * @param traversal Traversal to apply the strategy to
     */
    @Override
    public void apply(Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal) || !traversal.getGraph().isPresent()) {
            return;
        }

        Graph graph = traversal.getGraph().get();

        if (graph instanceof MastikGraph) {
            MastikGraph mastikGraph = (MastikGraph) traversal.getGraph().get();

            this.strategy.apply(traversal, mastikGraph.getBackend());
        }
    }
}
