package org.mastik.process.vertex;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.mastik.Backend;
import org.mastik.Strategy;

/**
 * Finds {@link VertexStep}s and replaces them with {@link MastikVertexStep}s
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since 1/6/17
 */
public class MastikVertexStepStrategy implements Strategy {

    @Override
    public void apply(Traversal.Admin traversal, Backend backend) {
        TraversalHelper.getStepsOfAssignableClassRecursively(VertexStep.class, traversal)
                .forEach(vertexStep -> {
                    MastikVertexStep mastikVertexStep = MastikVertexStep.fromVertexStep(vertexStep, backend);
                    TraversalHelper.replaceStep(vertexStep, mastikVertexStep, traversal);
                });
    }
}
