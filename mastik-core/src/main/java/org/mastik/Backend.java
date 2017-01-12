package org.mastik;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.mastik.query.Query;
import org.mastik.query.VertexQuery;

import java.util.Set;
import java.util.stream.Stream;

/**
 * A backend provides access to a remote data store
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since 12/28/16
 */
public interface Backend {

    /**
     * A {@link Query} which is not pointed to any specific element in the graph
     * @param query A org.mastik.structure.query to lookup
     * @param <E> Type of result
     * @return Returns a stream of org.mastik.structure.query results
     */
    <E extends Element> Stream<E> query(Query<E> query);

    /**
     * A {@link Query} which is centered upon a specific set of vertices, which returns edges
     * @param query A org.mastik.structure.query to lookup
     * @return Returns a stream of edges which are the org.mastik.structure.query results
     */
    Stream<Edge> queryVertex(VertexQuery query);

    /**
     * Given a set of vertex-ids, returns a {@link Stream} of {@link Vertex} instances,
     * without any properties. When the first property of one of the vertices is fetched,
     * loads the properties of all of the given vertices in a single bulk
     * @param vertexIds Set of vertex ids to retrieve
     * @return Stream of vertex instances
     */
    Stream<Vertex> getVerticesDeferred(Set<Object> vertexIds);
}
