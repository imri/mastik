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
import org.mastik.process.TraversalPredicatesCollector;
import org.mastik.query.PredicatesTree;
import org.mastik.query.Query;
import org.mastik.query.VertexQuery;
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

    public MastikGraphStep(Traversal.Admin traversal, Class<E> returnClass, boolean isStartStep, Set<Object> elementIds,
                           int limit, PredicatesTree predicates, List<Pair<String, Order>> orders, Backend backend) {
        super(traversal, returnClass, isStartStep, elementIds.toArray());

        ElementHelper.validateMixedElementIds(returnClass, elementIds);

        this.returnClass = returnClass;
        this.limit = limit;
        this.predicates = createInitialPredicatesTree(predicates, elementIds);
        this.orders = orders;
        this.backend = backend;

        this.setIteratorSupplier(this::process);
    }

    /**
     * Creates the initial predicates-tree, containing a predicate of ids and the given predicates tree
     */
    private static PredicatesTree createInitialPredicatesTree(PredicatesTree predicates, Set<Object> elementIds) {

        PredicatesTree idsPredicate = ElementUtils.createIdsPredicate(elementIds);
        return PredicatesTree.and(idsPredicate, predicates);
    }

    /**
     * Creates a {@link MastikGraphStep}  from a {@link GraphStep}
     *
     * @param graphStep VertexStep to extract arguments from
     * @param <E>        Type of step results
     * @return New instance of VertexStep
     */
    public static <S, E extends Element> MastikGraphStep<S, E> fromGraphStep(GraphStep<S, E> graphStep, Backend backend) {
        PredicatesTree predicates = TraversalPredicatesCollector.collect(graphStep, graphStep.getTraversal());

        // @todo: collect order and limit
        return new MastikGraphStep<>(graphStep.getTraversal(), graphStep.getReturnClass(), graphStep.isStartStep(),
                Sets.newHashSet(graphStep.getIds()), VertexQuery.noLimit(), predicates, VertexQuery.noOrders(), backend);
    }

    /**
     * Queries the backend for results
     */
    protected Iterator<E> process() {
        Query<E> query = new Query<>(this.returnClass, this.predicates, this.limit, Query.allProperties(), this.orders);

        return this.backend.query(query).iterator();
    }
}
