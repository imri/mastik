package org.mastik;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.mastik.query.PredicatesTree;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Utility functions related to elements
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since 1/6/17
 */
public class ElementUtils {
    /**
     * Given an edge of a vertex, extracts the vertex residing in the given direction from the edge
     */
    public static Vertex extractVertexFromEdge(Edge edge, Vertex vertex, Direction direction) {
        switch (direction) {
            case OUT:
                return edge.inVertex();
            case IN:
                return edge.outVertex();
            case BOTH:
                Vertex outV = edge.outVertex();
                Vertex inV = edge.inVertex();
                if (vertex.id().equals(outV.id()))
                    return inV;
                return outV;
            default:
                throw new IllegalArgumentException(String.format("Direction '%s' is unsupported", direction.toString()));
        }
    }

    /**
     * Given a set of element ids, creates a predicate-tree with a predicate containing them
     * @param ids Set of element ids
     * @return New instance of predicate-tree with ids predicate
     */
    public static PredicatesTree createIdsPredicate(Set<Object> ids) {
        if (ids.isEmpty()) {
            return PredicatesTree.emptyTree();
        }

        List<Object> extractedIds = ids.stream()
                .map(id -> {
                    if (id instanceof Element)
                        return ((Element) id).id();
                    return id;
                })
                .collect(Collectors.toList());

        HasContainer idPredicate = new HasContainer(T.id.getAccessor(), P.within(extractedIds));
        return PredicatesTree.createFromPredicates(idPredicate);
    }

    /**
     * Whether the given class is a Vertex
     *
     * @return True if it is a vertex, false otherwise
     */
    public static boolean isVertex(Class cls) {
        return Vertex.class.isAssignableFrom(cls);
    }

    /**
     * Whether the given class is a Edge
     *
     * @return True if it is an edge, false otherwise
     */
    public static boolean isEdge(Class cls) {
        return Edge.class.isAssignableFrom(cls);
    }
}
