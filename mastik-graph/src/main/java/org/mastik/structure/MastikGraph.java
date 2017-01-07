package org.mastik.structure;

import com.google.common.collect.Sets;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.mastik.Backend;
import org.mastik.ElementUtils;
import org.mastik.structure.base.BaseMastikGraph;
import org.mastik.query.PredicatesTree;
import org.mastik.query.Query;

import java.util.stream.Stream;

/**
 * A standard implementation of a graph
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since  1/6/17
 */
public class MastikGraph extends BaseMastikGraph {

    private final TraversalStrategies strategies;

    public MastikGraph(Backend backend, TraversalStrategies strategies) {
        super(backend);

        this.strategies = strategies;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<Vertex> verticesStream(Object... ids) {
        return queryIds(Vertex.class, ids);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<Edge> edgesStream(Object... ids) {
        return queryIds(Edge.class, ids);
    }

    private <E extends Element> Stream<E> queryIds(Class<E> returnType, Object[] ids) {
        ElementHelper.validateMixedElementIds(returnType, ids);

        PredicatesTree idPredicate = ElementUtils.createIdsPredicate(Sets.newHashSet(ids));
        Query<E> query = new Query<>(returnType, idPredicate, -1, null, null);

        return backend().query(query);
    }

    @Override
    public GraphTraversalSource traversal() {
        return new GraphTraversalSource(this, this.strategies);
    }
}
