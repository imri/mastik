package org.mastik.structure.base;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.mastik.structure.MastikBackend;

import java.util.Iterator;
import java.util.stream.Stream;

/**
 * A base class the graph
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since  1/6/17
 */
public abstract class BaseMastikGraph implements Graph {

    private static final Configuration EMPTY_CONFIGURATION = new PropertiesConfiguration();

    private MastikBackend backend;

    public BaseMastikGraph(MastikBackend backend) {
        this.backend = backend;
    }

    /**
     * Get the {@link Vertex} objects in this graph with the provided vertex ids or {@link Vertex} objects themselves.
     * If no ids are provided, get all vertices.  Note that a vertex identifier does not need to correspond to the
     * actual id used in the graph.  It needs to be a bit more flexible than that in that given the
     * {@link Graph.Features} around id support, multiple arguments might be applicable here.
     * <p/>
     * If the graph return {@code true} for {@link Features.VertexFeatures#supportsNumericIds()} then it should support
     * filters as with:
     * <ul>
     * <li>g.vertices(v)</li>
     * <li>g.vertices(v.id())</li>
     * <li>g.vertices(1)</li>
     * <li>g.vertices(1L)</li>
     * <li>g.vertices(1.0d)</li>
     * <li>g.vertices(1.0f)</li>
     * <li>g.vertices("1")</li>
     * </ul>
     * <p/>
     * If the graph return {@code true} for {@link Features.VertexFeatures#supportsCustomIds()} ()} then it should support
     * filters as with:
     * <ul>
     * <li>g.vertices(v)</li>
     * <li>g.vertices(v.id())</li>
     * <li>g.vertices(v.id().toString())</li>
     * </ul>
     * <p/>
     * If the graph return {@code true} for {@link Features.VertexFeatures#supportsAnyIds()} ()} then it should support
     * filters as with:
     * <ul>
     * <li>g.vertices(v)</li>
     * <li>g.vertices(v.id())</li>
     * </ul>
     * <p/>                                                                                                         
     * If the graph return {@code true} for {@link Features.VertexFeatures#supportsStringIds()} ()} then it should support
     * filters as with:
     * <ul>
     * <li>g.vertices(v)</li>
     * <li>g.vertices(v.id().toString())</li>
     * <li>g.vertices("id")</li>
     * </ul>
     * <p/>
     * If the graph return {@code true} for {@link Features.EdgeFeatures#supportsStringIds()} ()} then it should support
     * filters as with:
     * <ul>
     * <li>g.vertices(v)</li>
     * <li>g.vertices(v.id().toString())</li>
     * <li>g.vertices("id")</li>
     * </ul>
     *
     * @param vertexIds the ids of the vertices to get
     * @return an {@link Iterator} of vertices that match the provided vertex ids
     */
    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        return this.verticesStream(vertexIds).iterator();
    }

    /**
     * Get the {@link Vertex} objects in this graph with the provided vertex ids or {@link Vertex} objects themselves.
     * If no ids are provided, get all vertices.  Note that a vertex identifier does not need to correspond to the
     * actual id used in the graph.  It needs to be a bit more flexible than that in that given the
     * {@link Graph.Features} around id support, multiple arguments might be applicable here.
     * <p/>
     * If the graph return {@code true} for {@link Features.VertexFeatures#supportsNumericIds()} then it should support
     * filters as with:
     * <ul>
     * <li>g.vertices(v)</li>
     * <li>g.vertices(v.id())</li>
     * <li>g.vertices(1)</li>
     * <li>g.vertices(1L)</li>
     * <li>g.vertices(1.0d)</li>
     * <li>g.vertices(1.0f)</li>
     * <li>g.vertices("1")</li>
     * </ul>
     * <p/>
     * If the graph return {@code true} for {@link Features.VertexFeatures#supportsCustomIds()} ()} then it should support
     * filters as with:
     * <ul>
     * <li>g.vertices(v)</li>
     * <li>g.vertices(v.id())</li>
     * <li>g.vertices(v.id().toString())</li>
     * </ul>
     * <p/>
     * If the graph return {@code true} for {@link Features.VertexFeatures#supportsAnyIds()} ()} then it should support
     * filters as with:
     * <ul>
     * <li>g.vertices(v)</li>
     * <li>g.vertices(v.id())</li>
     * </ul>
     * <p/>                                                                                                         
     * If the graph return {@code true} for {@link Features.VertexFeatures#supportsStringIds()} ()} then it should support
     * filters as with:
     * <ul>
     * <li>g.vertices(v)</li>
     * <li>g.vertices(v.id().toString())</li>
     * <li>g.vertices("id")</li>
     * </ul>
     * <p/>
     * If the graph return {@code true} for {@link Features.EdgeFeatures#supportsStringIds()} ()} then it should support
     * filters as with:
     * <ul>
     * <li>g.vertices(v)</li>
     * <li>g.vertices(v.id().toString())</li>
     * <li>g.vertices("id")</li>
     * </ul>
     *
     * @param vertexIds the ids of the vertices to get
     * @return a {@link Stream} of vertices that match the provided vertex ids
     */
    public abstract Stream<Vertex> verticesStream(Object... vertexIds);

    /**
     * Get the {@link Edge} objects in this graph with the provided edge ids or {@link Edge} objects. If no ids are
     * provided, get all edges. Note that an edge identifier does not need to correspond to the actual id used in the
     * graph.  It needs to be a bit more flexible than that in that given the {@link Graph.Features} around id support,
     * multiple arguments might be applicable here.
     * <p/>
     * If the graph return {@code true} for {@link Features.EdgeFeatures#supportsNumericIds()} then it should support
     * filters as with:
     * <ul>
     * <li>g.edges(e)</li>
     * <li>g.edges(e.id())</li>
     * <li>g.edges(1)</li>
     * <li>g.edges(1L)</li>
     * <li>g.edges(1.0d)</li>
     * <li>g.edges(1.0f)</li>
     * <li>g.edges("1")</li>
     * </ul>
     * <p/>
     * If the graph return {@code true} for {@link Features.EdgeFeatures#supportsCustomIds()} ()} then it should support
     * filters as with:
     * <ul>
     * <li>g.edges(e)</li>
     * <li>g.edges(e.id())</li>
     * <li>g.edges(e.id().toString())</li>
     * </ul>
     * <p/>
     * If the graph return {@code true} for {@link Features.EdgeFeatures#supportsAnyIds()} ()} then it should support
     * filters as with:
     * <ul>
     * <li>g.edges(e)</li>
     * <li>g.edges(e.id())</li>
     * </ul>
     * <p/>
     * If the graph return {@code true} for {@link Features.EdgeFeatures#supportsStringIds()} ()} then it should support
     * filters as with:
     * <ul>
     * <li>g.edges(e)</li>
     * <li>g.edges(e.id().toString())</li>
     * <li>g.edges("id")</li>
     * </ul>
     *
     * @param edgeIds the ids of the edges to get
     * @return an {@link Iterator} of edges that match the provided edge ids
     */
    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        return this.edgesStream(edgeIds).iterator();
    }

    /**
     * Get the {@link Edge} objects in this graph with the provided edge ids or {@link Edge} objects. If no ids are
     * provided, get all edges. Note that an edge identifier does not need to correspond to the actual id used in the
     * graph.  It needs to be a bit more flexible than that in that given the {@link Graph.Features} around id support,
     * multiple arguments might be applicable here.
     * <p/>
     * If the graph return {@code true} for {@link Features.EdgeFeatures#supportsNumericIds()} then it should support
     * filters as with:
     * <ul>
     * <li>g.edges(e)</li>
     * <li>g.edges(e.id())</li>
     * <li>g.edges(1)</li>
     * <li>g.edges(1L)</li>
     * <li>g.edges(1.0d)</li>
     * <li>g.edges(1.0f)</li>
     * <li>g.edges("1")</li>
     * </ul>
     * <p/>
     * If the graph return {@code true} for {@link Features.EdgeFeatures#supportsCustomIds()} ()} then it should support
     * filters as with:
     * <ul>
     * <li>g.edges(e)</li>
     * <li>g.edges(e.id())</li>
     * <li>g.edges(e.id().toString())</li>
     * </ul>
     * <p/>
     * If the graph return {@code true} for {@link Features.EdgeFeatures#supportsAnyIds()} ()} then it should support
     * filters as with:
     * <ul>
     * <li>g.edges(e)</li>
     * <li>g.edges(e.id())</li>
     * </ul>
     * <p/>
     * If the graph return {@code true} for {@link Features.EdgeFeatures#supportsStringIds()} ()} then it should support
     * filters as with:
     * <ul>
     * <li>g.edges(e)</li>
     * <li>g.edges(e.id().toString())</li>
     * <li>g.edges("id")</li>
     * </ul>
     *
     * @param edgeIds the ids of the edges to get
     * @return a {@link Stream} of edges that match the provided edge ids
     */
    public abstract Stream<Edge> edgesStream(Object... edgeIds);

    /**
     * Add a {@link Vertex} to the graph given an optional series of key/value pairs.  These key/values
     * must be provided in an even number where the odd numbered arguments are {@link String} property keys and the
     * even numbered arguments are the related property values.
     *
     * @param keyValues The key/value pairs to turn into vertex properties
     * @return The newly created vertex
     */
    @Override
    public Vertex addVertex(final Object... keyValues) {
        throw BaseMastikElement.readonlyGraphException();
    }

    /**
     * Gets the backend, used for fetching graph data from store
     */
    protected MastikBackend backend() {
        return backend;
    }

    /**
     * Configure and control the transactions for those graphs that support this feature.  Note that this method does
     * not indicate the creation of a "transaction" object.  A {@link Transaction} in the TinkerPop context is a
     * transaction "factory" or "controller" that helps manage transactions owned by the underlying graph database.
     */
    @Override
    public Transaction tx() {
        throw Exceptions.transactionsNotSupported();
    }

    /**
     * Closing a {@code Graph} is equivalent to "shutdown" and implies that no futher operations can be executed on
     * the instance.  Users should consult the documentation of the underlying graph database implementation for what
     * this "shutdown" will mean as it pertains to open transactions.  It will typically be the end user's
     * responsibility to synchronize the thread that calls {@code close()} with other threads that are accessing open
     * transactions. In other words, be sure that all work performed on the {@code Graph} instance is complete prior
     * to calling this method.
     */
    @Override
    public void close() throws Exception {

    }

    /**
     * A collection of global {@link Variables} associated with the graph.
     * Variables are used for storing metadata about the graph.
     *
     * @return The variables associated with this graph
     */
    @Override
    public Variables variables() {
        throw Exceptions.variablesNotSupported();
    }

    /**
     * Get the {@link org.apache.commons.configuration.Configuration} associated with the construction of this graph.
     * Whatever configuration was passed to {@link GraphFactory#open(org.apache.commons.configuration.Configuration)}
     * is what should be returned by this method.
     *
     * @return the configuration used during graph construction.
     */
    @Override
    public Configuration configuration() {
        return EMPTY_CONFIGURATION;
    }

    /**
     * Generate a {@link GraphComputer} using the default engine of the underlying graph system.
     * This is a shorthand method for the more involved method that uses {@link Graph#compute(Class)}.
     *
     * @return A default graph computer
     * @throws IllegalArgumentException if there is no default graph computer
     */
    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        throw Exceptions.graphComputerNotSupported();
    }

    /**
     * Generate a {@link GraphComputer} using the default engine of the underlying graph system.
     * This is a shorthand method for the more involved method that uses {@link Graph#compute(Class)}.
     *
     * @return A default graph computer
     * @throws IllegalArgumentException if there is no default graph computer
     */
    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Exceptions.graphComputerNotSupported();
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, getClass().getName());
    }
}
