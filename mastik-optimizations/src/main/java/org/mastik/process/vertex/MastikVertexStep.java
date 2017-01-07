package org.mastik.process.vertex;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;
import org.mastik.Backend;
import org.mastik.ElementUtils;
import org.mastik.StreamUtils;
import org.mastik.process.BulkStep;
import org.mastik.process.TraversalPredicatesCollector;
import org.mastik.query.PredicatesTree;
import org.mastik.query.VertexQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Mastik implementation of {@link VertexStep}
 *
 * Per each {@link Traverser}s bulk, extracts the inner {@link Vertex} objects,
 * performs a {@link VertexQuery}, then maps the results back to the traversers
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since 1/6/17
 */
public class MastikVertexStep<E extends Element> extends BulkStep<Vertex, E> {
    private static final Logger logger = LoggerFactory.getLogger(MastikVertexStep.class);

    private final Class<E> returnClass;
    private final Direction direction;
    private final Set<String> edgeLabels;
    private int limit;
    private PredicatesTree predicates;
    private List<Pair<String, Order>> orders;
    private Backend backend;

    public MastikVertexStep(Traversal.Admin traversal, Class<E> returnClass, Direction direction, Set<String> edgeLabels,
                            int limit, PredicatesTree predicates, List<Pair<String, Order>> orders, Backend backend) {
        super(traversal);

        this.returnClass = returnClass;
        this.direction = direction;
        this.edgeLabels = edgeLabels;
        this.limit = limit;
        this.predicates = predicates;
        this.orders = orders;
        this.backend = backend;
    }

    /**
     * Creates a {@link MastikVertexStep}  from a {@link VertexStep}
     *
     * @param vertexStep VertexStep to extract arguments from
     * @param <E>        Type of step results
     * @return New instance of VertexStep
     */
    public static <E extends Element> MastikVertexStep<E> fromVertexStep(VertexStep<E> vertexStep, Backend backend) {
        PredicatesTree predicates = TraversalPredicatesCollector.collect(vertexStep, vertexStep.getTraversal());

        // @todo: collect order and limit
        return new MastikVertexStep<>(vertexStep.getTraversal(), vertexStep.getReturnClass(), vertexStep.getDirection(),
                vertexStep.getLabels(), VertexQuery.noLimit(), predicates, VertexQuery.noOrders(), backend);
    }

    @Override
    protected Stream<Traverser.Admin<E>> process(List<Traverser.Admin<Vertex>> traversers) {
        Set<Vertex> vertices = traversers.stream().map(Traverser::get).collect(Collectors.toSet());

        Stream<Traverser.Admin<E>> traversersGenerator = fetchTraversersEdges(traversers, vertices);

        if (this.returnsVertex()) {
            fetchTraversersProperties(traversers);
        }

        return traversersGenerator;
    }

    private void fetchTraversersProperties(List<Traverser.Admin<Vertex>> traversers) {
        // @todo: get properties
    }

    private Stream<Traverser.Admin<E>> fetchTraversersEdges(List<Traverser.Admin<Vertex>> traversers, Set<Vertex> vertices) {
        Map<Object, List<Traverser.Admin<Vertex>>> traversersByVertexId = traversers.stream()
                .collect(Collectors.groupingBy(traverser -> traverser.get().id()));

        VertexQuery query = new VertexQuery(vertices, this.direction, this.predicates, this.limit, this.edgeLabels, this.orders);
        return this.backend.queryVertex(query)
                .flatMap(edge -> getEdgeTraversers(edge, traversersByVertexId));

    }

    private Stream<Traverser.Admin<E>> getEdgeTraversers(Edge edge, Map<Object, List<Traverser.Admin<Vertex>>> traversersByVertexId) {
        return StreamUtils.toStream(edge.vertices(this.direction))
                .filter(vertex -> traversersByVertexId.containsKey(vertex.id()))
                .flatMap(vertex -> traversersByVertexId.get(vertex.id()).stream()
                        .map(traverser -> traverser.split(getReturnElement(edge, vertex), this)))
                .filter(Objects::nonNull);
    }

    private E getReturnElement(Edge edge, Vertex vertex) {
        if (!this.returnsVertex()) {
            return (E) edge;
        }

        return (E) ElementUtils.extractVertexFromEdge(edge, vertex, this.direction);
    }

    /**
     * Whether this step returns vertices
     *
     * @return True if returns vertices, false otherwise
     */
    private boolean returnsVertex() {
        return Vertex.class.isAssignableFrom(this.returnClass);
    }
}
