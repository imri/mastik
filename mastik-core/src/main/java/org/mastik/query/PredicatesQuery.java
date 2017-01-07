package org.mastik.query;

import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;

/**
 * A query which involves predicates test
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since  1/6/17
 */
public class PredicatesQuery<E extends Element> {
    private final PredicatesTree predicates;

    public PredicatesQuery(PredicatesTree predicates) {
        this.predicates = predicates;
    }

    /**
     * Returns the predicates tree of this query
     */
    public PredicatesTree getPredicates() {
        return this.predicates;
    }

    /**
     * Tests the element against the predicates tree
     * @param element Element to test
     * @param predicates Predicates to test against
     * @return True if the element passed the predicates, false otherwise
     */
    public boolean test(E element, PredicatesTree predicates) {
        if (predicates.isEmpty()) {
            return true;
        }

        if (predicates.isAnd()) {
            if (predicates.hasPredicates()) {
                if (!HasContainer.testAll(element, Lists.newLinkedList(predicates.predicates()))) {
                    return false;
                }
            }

            if (predicates.hasChildren()) {
                for (PredicatesTree childContainer : predicates.children()) {
                    if (!test(element, childContainer)) {
                        return false;
                    }
                }
            }

            return true;
        } else if (predicates.isOr()) {
            if (predicates.hasPredicates()) {
                for (HasContainer predicate : predicates.predicates()) {
                    if (predicate.test(element)) {
                        return true;
                    }
                }
            }

            if (predicates.hasChildren()) {
                for (PredicatesTree child : predicates.children()) {
                    if (test(element, child)) {
                        return true;
                    }
                }
            }

            return false;
        } else {
            throw new IllegalStateException(String.format("Unexpected predicates-tree '%s' was given", predicates));
        }
    }

    @Override
    public String toString() {
        return String.format("PredicatesQuery{predicates=%s}", this.predicates);
    }
}
