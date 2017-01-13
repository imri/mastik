package org.mastik.process;

import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.mastik.query.PredicatesTree;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Collects predicates, limits etc from a traversal
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since 1/6/17
 */
public class TraversalCollector {
    public static PredicatesTree collectPredicates(Step step) {
        List<PredicatesTree> predicates = Lists.newArrayList();

        for (Step currentStep = step.getNextStep(); currentStep != null; currentStep = currentStep.getNextStep()) {
            if (currentStep instanceof HasContainerHolder) {

                predicates.addAll(((HasContainerHolder) currentStep).getHasContainers().stream()
                        .map(PredicatesTree::createFromPredicates)
                        .collect(Collectors.toList()));

                currentStep.getTraversal().removeStep(currentStep);
            } else if (!(currentStep instanceof RangeGlobalStep)) {
                break;
            }
        }

        return PredicatesTree.and(predicates);
    }
}
