package org.mastik.structure;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.mastik.Backend;
import org.mastik.structure.base.BaseMastikVertex;
import org.mastik.query.PredicatesTree;
import org.mastik.query.VertexQuery;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A standard implementation of a vertex
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since  1/6/17
 */
public class MastikVertex extends BaseMastikVertex {

    public MastikVertex(String id, Map<String, VertexProperty> properties, Backend backend) {
        super(id, properties, backend);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<Edge> edgesStream(Direction direction, String... edgeLabels) {
        PredicatesTree predicatesContainer;

        if (edgeLabels.length > 0) {
            HasContainer labelsPredicate = new HasContainer(T.label.getAccessor(), P.within(edgeLabels));
            predicatesContainer = PredicatesTree.createFromPredicates(labelsPredicate);
        } else {
            predicatesContainer = PredicatesTree.emptyTree();
        }

        VertexQuery searchVertexQuery = new VertexQuery(Collections.singletonList(this), direction, predicatesContainer, -1, null, null);
        return backend().queryVertex(searchVertexQuery);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<Vertex> verticesStream(Direction direction, String... edgeLabels) {
        return this.edgesStream(direction, edgeLabels)
                .map(edge -> extractVertexFromEdge(edge, direction));
    }

    /**
     * Given an edge of this vertex, extracts the vertex residing in the given direction from the edge
     */
    private Vertex extractVertexFromEdge(Edge edge, Direction direction) {
        switch (direction) {
            case OUT:
                return edge.inVertex();
            case IN:
                return edge.outVertex();
            case BOTH:
                Vertex outV = edge.outVertex();
                Vertex inV = edge.inVertex();
                if (this.id().equals(outV.id()))
                    return inV;
                return outV;
            default:
                throw new IllegalArgumentException(String.format("Direction '%s' is unsupported", direction.toString()));
        }
    }
}