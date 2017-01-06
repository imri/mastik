package org.mastik.structure.base;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.mastik.Backend;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A base class for all edges in Mastik
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since  1/6/17
 */
public abstract class BaseMastikEdge extends BaseMastikElement implements Edge {
    public BaseMastikEdge(String id, String label, Map properties, Backend backend) {
        super(id, label, properties, backend);
    }

    /**
     * Retrieve the vertex (or vertices) associated with this edge as defined by the direction.
     * If the direction is {@link Direction#BOTH} then the iterator order is: {@link Direction#OUT} then {@link Direction#IN}.
     *
     * @param direction Get the incoming vertex, outgoing vertex, or both vertices
     * @return An iterator with 1 or 2 vertices
     */
    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        return this.verticesStream(direction).iterator();
    }

    /**
     * Retrieve the vertex (or vertices) associated with this edge as defined by the direction.
     * If the direction is {@link Direction#BOTH} then the iterator order is: {@link Direction#OUT} then {@link Direction#IN}.
     *
     * @param direction Get the incoming vertex, outgoing vertex, or both vertices
     * @return A stream with 1 or 2 vertices
     */
    public abstract Stream<Vertex> verticesStream(Direction direction);

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }
}
