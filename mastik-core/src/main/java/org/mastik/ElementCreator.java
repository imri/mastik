package org.mastik;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Map;

/**
 * Encapsulating creation of concrete Graph elements
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since 1/6/17
 */
public interface ElementCreator {
    /**
     * Given a vertex-id, creates an instance of Vertex from it
     */
    Vertex createVertexFromId(String vertexId, Backend backend);

    /**
     * Creates a new instance of an Edge given raw arguments
     */
    Edge createEdge(String edgeId, String label, Map<String, Object> properties, Vertex outVertex, Vertex inVertex, Backend backend);

    /**
     * Creates a new instance of a Vertex given raw arguments
     */
    Vertex createVertex(String vertexId, Map<String, Object> properties, Backend backend);
}
