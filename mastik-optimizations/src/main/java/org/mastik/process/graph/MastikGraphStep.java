package org.mastik.process.graph;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.javatuples.Pair;
import org.mastik.Backend;
import org.mastik.ElementUtils;
import org.mastik.process.TraversalCollector;
import org.mastik.query.PredicatesTree;
import org.mastik.query.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Mastik implementation of {@link GraphStep}
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since 1/7/17
 */
public class MastikGraphStep<S, E extends Element> extends GraphStep<S, E> {
    private static final Logger logger = LoggerFactory.getLogger(MastikGraphStep.class);

    private final Class<E> returnClass;
    private int limit;
    private PredicatesTree predicates;
    private List<Pair<String, Order>> orders;
    private Backend backend;
    private Set<Object> elementIds;

    public MastikGraphStep(Traversal.Admin traversal, Class<E> returnClass, boolean isStartStep, Set<Object> elementIds,
                           int limit, PredicatesTree predicates, List<Pair<String, Order>> orders, Backend backend) {
        super(traversal, returnClass, isStartStep, elementIds.toArray());

        ElementHelper.validateMixedElementIds(returnClass, elementIds);

        this.elementIds = elementIds;
        this.returnClass = returnClass;
        this.limit = limit;
        this.predicates = predicates;
        this.orders = orders;
        this.backend = backend;

        this.setIteratorSupplier(this::process);
    }

    /**
     * Creates a {@link MastikGraphStep}  from a {@link GraphStep}
     *
     * @param graphStep VertexStep to extract arguments from
     * @param <E>       Type of step results
     * @return New instance of VertexStep
     */
    public static <S, E extends Element> MastikGraphStep<S, E> fromGraphStep(GraphStep<S, E> graphStep, Backend backend) {
        PredicatesTree predicates = TraversalCollector.collectPredicates(graphStep);

        // @todo: collect order and limit
        return new MastikGraphStep<>(graphStep.getTraversal(), graphStep.getReturnClass(), graphStep.isStartStep(),
                Sets.newHashSet(graphStep.getIds()), Query.noLimit(), predicates, Query.noOrders(), backend);
    }

    /**
     * Queries the backend for results
     */
    protected Iterator<E> process() {

        if (this.returnsVertex() && this.canCreateDeferredVertices()) {
            return (Iterator)this.backend.getVerticesDeferred(this.elementIds).iterator();
        } else {
            PredicatesTree idsPredicate = ElementUtils.createIdsPredicate(this.elementIds);
            PredicatesTree mergedPredicates = PredicatesTree.and(idsPredicate, this.predicates);

            Query<E> query = new Query<>(this.returnClass, mergedPredicates, this.limit, Query.allLabels(), this.orders);

            return this.backend.query(query).iterator();
        }
    }

    /**
     * If no predicates, limit and orders were set,
     * this will return true. Otherwise, it'll return false.
     */
    private boolean canCreateDeferredVertices() {
        return !this.elementIds.isEmpty() && this.predicates.isEmpty() && this.limit == Query.noLimit() && this.orders == Query.noOrders();
    }
}
