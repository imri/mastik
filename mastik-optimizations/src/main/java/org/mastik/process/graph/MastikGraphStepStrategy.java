package org.mastik.process.graph;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.mastik.Backend;
import org.mastik.Strategy;

/**
 * Finds {@link GraphStep}s and replaces them with {@link MastikGraphStep}s
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since 1/7/17
 */
public class MastikGraphStepStrategy implements Strategy {

    @Override
    public void apply(Traversal.Admin traversal, Backend backend) {
        TraversalHelper.getStepsOfAssignableClassRecursively(GraphStep.class, traversal)
                .forEach(graphStep -> {
                    MastikGraphStep mastikGraphStep = MastikGraphStep.fromGraphStep(graphStep, backend);
                    TraversalHelper.replaceStep(graphStep, mastikGraphStep, traversal);
                });
    }
}
