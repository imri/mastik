package org.mastik;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.mastik.structure.MastikEdge;
import org.mastik.structure.MastikProperty;
import org.mastik.structure.MastikVertex;
import org.mastik.structure.MastikVertexProperty;

import java.util.Collections;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Creates Mastik elements (MastikVertex, MastikEdge, etc) from given arguments
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since 1/6/17
 */
public class MastikElementCreator implements ElementCreator {
    /**
     * {@inheritDoc}
     */
    @Override
    public Vertex createVertexFromId(String vertexId, Backend backend) {
        return new MastikVertex(vertexId, Collections.emptyMap(), backend);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Edge createEdge(String edgeId, String label, Map<String, Object> fields, Vertex outVertex, Vertex inVertex, Backend backend) {
        Map<String, Property> properties = fields.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> createProperty(entry.getKey(), entry.getValue())));

        return new MastikEdge(edgeId, label, properties, outVertex, inVertex, backend);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Vertex createVertex(String vertexId, Map<String, Object> fields, Backend backend) {
        Map<String, VertexProperty> properties = fields.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey,
                        entry -> createVertexProperty(entry.getKey(), entry.getValue())));

        return new MastikVertex(vertexId, properties, backend);
    }

    /**
     * Creates a property given raw key and value
     */
    protected Property createProperty(String key, Object value) {
        ElementHelper.validateProperty(key, value);

        return new MastikProperty<>(key, value);
    }

    /**
     * Creates a vertex property given raw key and value
     */
    protected VertexProperty createVertexProperty(String key, Object value) {
        ElementHelper.validateProperty(key, value);

        return new MastikVertexProperty<>(key, value);
    }
}
