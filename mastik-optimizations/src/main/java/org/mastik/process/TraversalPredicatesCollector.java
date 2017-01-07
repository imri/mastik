package org.mastik.process;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.mastik.query.PredicatesTree;

/**
 * Collects step predicates from a traversal
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since 1/6/17
 */
public class TraversalPredicatesCollector {
    public static PredicatesTree collect(Step step, Traversal.Admin traversal) {
        return PredicatesTree.emptyTree(); // @todo: implement collector
    }
}
