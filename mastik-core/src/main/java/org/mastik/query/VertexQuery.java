package org.mastik.query;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.javatuples.Pair;

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

    private final Set<Object> vertexIds;
    private final Direction direction;

    public VertexQuery(Set<Object> vertexIds, Direction direction, PredicatesTree predicatesContainer, int limit, Set<String> labels, List<Pair<String, Order>> orders) {
        super(Edge.class, predicatesContainer, limit, labels, orders);

        this.vertexIds = vertexIds;
        this.direction = direction;
    }

    /**
     * Returns the ids of vertices that their edges queried
     */
    public Set<Object> getVertexIds() {
        return Collections.unmodifiableSet(vertexIds);
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
            if (vertexIds.contains(element.outVertex().id())) {
                return true;
            }
        }

        if (direction == Direction.IN || direction == Direction.BOTH) {
            if (vertexIds.contains(element.inVertex().id())) {
                return true;
            }
        }

        return false;
    }

    @Override
    public String toString() {
        return String.format("VertexQuery{vertices=%s, direction=%s, limit=%s}", this.vertexIds, this.direction, this.getLimit());
    }
}
