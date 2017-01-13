package org.mastik.process.vertex;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
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
        applyRecursively(traversal, backend);
    }

    /**
     * Given a traversal, replaces it's child {@link VertexStep}s with {@link MastikVertexStep}s,
     * then iterates over it's child {@link TraversalParent}s global and local children,
     * and applies recursively to them
     */
    private void applyRecursively(Traversal.Admin traversal, Backend backend) {
        TraversalHelper.getStepsOfAssignableClass(VertexStep.class, traversal)
                .forEach(vertexStep -> replaceVertexStep(traversal, vertexStep, backend));

        TraversalHelper.getStepsOfAssignableClass(TraversalParent.class, traversal)
                .forEach(traversalParent -> {

                    traversalParent.getGlobalChildren()
                            .forEach(innerTraversal -> applyRecursively(innerTraversal, backend));

                    traversalParent.getLocalChildren()
                            .forEach(innerTraversal -> applyRecursively(innerTraversal, backend));
                });
    }

    private void replaceVertexStep(Traversal.Admin traversal, VertexStep vertexStep, Backend backend) {
        MastikVertexStep mastikVertexStep = MastikVertexStep.fromVertexStep(vertexStep, backend);
        TraversalHelper.replaceStep(vertexStep, mastikVertexStep, traversal);
    }
}
