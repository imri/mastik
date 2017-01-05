package org.mastik.query;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.javatuples.Pair;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * An edges {@link Query} which is centered on a set of vertices
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since  1/6/17
 */
public class VertexQuery extends Query<Edge> {

    private final Collection<Vertex> vertices;
    private final Direction direction;

    public VertexQuery(Collection<Vertex> vertices, Direction direction, PredicatesTree predicatesContainer, int limit, Set<String> propertyKeys, List<Pair<String, Order>> orders) {
        super(Edge.class, predicatesContainer, limit, propertyKeys, orders);

        this.vertices = vertices;
        this.direction = direction;
    }

    /**
     * Returns the vertices that their edges queried
     */
    public Collection<Vertex> getVertices() {
        return Collections.unmodifiableCollection(vertices);
    }

    /**
     * Returns the requested edges direction
     */
    public Direction getDirection() {
        return direction;
    }

    @Override
    public boolean test(Edge element, PredicatesTree predicates) {
        if (!super.test(element, predicates)) {
            return false;
        }

        if (direction == Direction.OUT || direction == Direction.BOTH) {
            if (vertices.contains(element.outVertex())) {
                return true;
            }
        }

        if (direction == Direction.IN || direction == Direction.BOTH) {
            if (vertices.contains(element.inVertex())) {
                return true;
            }
        }

        return false;
    }

    @Override
    public String toString() {
        return String.format("VertexQuery{vertices=%s, direction=%s, limit=%s}", vertices, direction, getLimit());
    }
}
