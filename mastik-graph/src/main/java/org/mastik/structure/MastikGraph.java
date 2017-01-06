package org.mastik.structure;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.mastik.Backend;
import org.mastik.structure.base.BaseMastikGraph;
import org.mastik.query.PredicatesTree;
import org.mastik.query.Query;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A standard implementation of a graph
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since  1/6/17
 */
public class MastikGraph extends BaseMastikGraph {

    public MastikGraph(Backend backend) {
        super(backend);
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
        PredicatesTree idPredicate = createIdPredicate(ids, returnType);
        Query<E> query = new Query<>(returnType, idPredicate, -1, null, null);

        return backend().query(query);
    }

    public static <E extends Element> PredicatesTree createIdPredicate(Object[] ids, Class<E> returnType) {
        ElementHelper.validateMixedElementIds(returnType, ids);

        if (ids.length == 0) {
            return PredicatesTree.emptyTree();
        }

        List<Object> extractedIds = Stream.of(ids)
                .map(id -> {
                    if (id instanceof Element)
                        return ((Element) id).id();
                    return id;
                })
                .collect(Collectors.toList());

        HasContainer idPredicate = new HasContainer(T.id.getAccessor(), P.within(extractedIds));
        return PredicatesTree.createFromPredicates(idPredicate);
    }
}
