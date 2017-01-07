package org.mastik.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.mastik.Backend;
import org.mastik.structure.base.BaseMastikEdge;

import java.util.Map;
import java.util.stream.Stream;

/**
 * A standard implementation of an edge
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since  1/6/17
 */
public class MastikEdge extends BaseMastikEdge implements Edge {

    private Vertex inVertex;
    private Vertex outVertex;

    public MastikEdge(String id, String label, Map<String, Property> properties, Vertex outVertex, Vertex inVertex, Backend backend) {
        super(id, label, properties, backend);

        this.outVertex = outVertex;
        this.inVertex = inVertex;
    }

    /**
     * {@inheritDoc}
     */
    public Stream<Vertex> verticesStream(Direction direction) {
        if (direction == Direction.OUT)
            return Stream.of(outVertex);

        if (direction == Direction.IN)
            return Stream.of(inVertex);

        return Stream.of(outVertex, inVertex);
    }
}
