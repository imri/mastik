package org.mastik.structure.base;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.mastik.structure.MastikBackend;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

/**
 * A base class for vertices in Mastik
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since  1/6/17
 */
public abstract class BaseMastikVertex extends BaseMastikElement<VertexProperty> implements Vertex {

    public BaseMastikVertex(String id, Map<String, VertexProperty> properties, MastikBackend backend) {
        super(id, Vertex.DEFAULT_LABEL, properties, backend);
    }

    /**
     * Get the {@link VertexProperty} for the provided key. If the property does not exist, return
     * {@link VertexProperty#empty}. If there are more than one vertex properties for the provided
     * key, then throw {@link Vertex.Exceptions#multiplePropertiesExistForProvidedKey}.
     *
     * @param key the key of the vertex property to get
     * @param <V> the expected type of the vertex property value
     * @return the retrieved vertex property
     */
    @Override
    public <V> VertexProperty<V> property(String key) {
        return (VertexProperty)super.property(key);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        return (Iterator)super.properties(propertyKeys);
    }

    /**
     * Gets a {@link java.util.stream.Stream} of incident edges.
     *
     * @param direction  The incident direction of the edges to retrieve off this vertex
     * @param edgeLabels The labels of the edges to retrieve. If no labels are provided, then get all edges.
     * @return A stream of edges meeting the provided specification
     */
    public abstract Stream<Edge> edgesStream(final Direction direction, final String... edgeLabels);

    /**
     * Gets an {@link java.util.stream.Stream} of adjacent vertices.
     *
     * @param direction  The adjacency direction of the vertices to retrieve off this vertex
     * @param edgeLabels The labels of the edges associated with the vertices to retrieve. If no labels are provided,
     *                   then get all edges.
     * @return An stream of vertices meeting the provided specification
     */
    public abstract Stream<Vertex> verticesStream(final Direction direction, final String... edgeLabels);

    /**
     * Gets an {@link Iterator} of incident edges.
     *
     * @param direction  The incident direction of the edges to retrieve off this vertex
     * @param edgeLabels The labels of the edges to retrieve. If no labels are provided, then get all edges.
     * @return An iterator of edges meeting the provided specification
     */
    @Override
    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        return edgesStream(direction, edgeLabels).iterator();
    }

    /**
     * Gets an {@link Iterator} of adjacent vertices.
     *
     * @param direction  The adjacency direction of the vertices to retrieve off this vertex
     * @param edgeLabels The labels of the edges associated with the vertices to retrieve. If no labels are provided,
     *                   then get all edges.
     * @return An iterator of vertices meeting the provided specification
     */
    @Override
    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        return verticesStream(direction, edgeLabels).iterator();
    }


    /**
     * Set the provided key to the provided value using default {@link VertexProperty.Cardinality} for that key.
     * The default cardinality can be vendor defined and is usually tied to the graph schema.
     * The default implementation of this method determines the cardinality
     * {@code graph().features().vertex().getCardinality(key)}. The provided key/values are the properties of the
     * newly created {@link VertexProperty}. These key/values must be provided in an even number where the odd
     * numbered arguments are {@link String}.
     *
     * @param key       the key of the vertex property
     * @param value     The value of the vertex property
     * @param keyValues the key/value pairs to turn into vertex property properties
     * @param <V>       the type of the value of the vertex property
     * @return the newly created vertex property
     */
    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, final Object... keyValues) {
        throw readonlyGraphException();
    }
    /**
     * Set the provided key to the provided value using {@link VertexProperty.Cardinality#single}.
     *
     * @param key   the key of the vertex property
     * @param value The value of the vertex property
     * @param <V>   the type of the value of the vertex property
     * @return the newly created vertex property
     */
    @Override
    public <V> VertexProperty<V> property(String key, V value) {
        throw readonlyGraphException();
    }

    /**
     * Add an outgoing edge to the vertex with provided label and edge properties as key/value pairs.
     * These key/values must be provided in an even number where the odd numbered arguments are {@link String}
     * property keys and the even numbered arguments are the related property values.
     *
     * @param label     The label of the edge
     * @param inVertex  The vertex to receive an incoming edge from the current vertex
     * @param keyValues The key/value pairs to turn into edge properties
     * @return the newly created edge
     */
    @Override
    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        throw readonlyGraphException();
    }

    /**
     * Removes the {@code Element} from the graph.
     */
    @Override
    public void remove() {
        throw readonlyGraphException();
    }

}