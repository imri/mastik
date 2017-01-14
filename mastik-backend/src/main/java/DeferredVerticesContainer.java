import com.google.common.collect.Maps;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.mastik.Backend;
import org.mastik.StreamUtils;
import org.mastik.query.Query;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A lazy object that wraps a deferred vertex-query
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since 1/7/17
 */
class DeferredVerticesContainer {
    private Query<Vertex> verticesQuery;
    private Backend backend;
    private Map<Object, Map<String, VertexProperty>> verticesProperties;

    DeferredVerticesContainer(Query<Vertex> verticesQuery, Backend backend) {

        this.verticesQuery = verticesQuery;
        this.backend = backend;
        this.verticesProperties = null;
    }

    /**
     * Given a vertex-id, if the stored query was not executed,
     * executes the query and populated 'verticesProperties'.
     * Then returns the properties map of the given vertex
     * @param vertexId Id of a vertex to get its properties map
     * @return Properties map of the given vertex-id
     */
    private Map<String, VertexProperty> queryVertexProperties(Object vertexId) {
        if (this.verticesProperties == null) {
            query();
        }

        Map<String, VertexProperty> vertexProperties = this.verticesProperties.remove(vertexId);

        if (vertexProperties == null) {
            return Collections.emptyMap();
        }

        return vertexProperties;
    }

    /**
     * Runs the stored query and populates 'verticesProperties'
     */
    private void query() {
        this.verticesProperties = Maps.newHashMap();

        this.backend.query(this.verticesQuery)
            .forEach(vertex -> this.verticesProperties.put(vertex.id(), StreamUtils.toStream(vertex.properties())
                    .collect(Collectors.toMap(Property::key, property -> property))));
    }

    /**
     * Given a vertex-id, creates a properties map,
     * which internally invokes 'queryVertexProperties' once a property is first requested
     * @param vertexId Id of the vertex
     * @return New instance of properties map
     */
    public Map<String, VertexProperty> makeVertexPropertiesMap(Object vertexId) {
        return new Map<String, VertexProperty>() {
            /**
             * Properties of the vertex
             */
            private Map<String, VertexProperty> properties;

            /**
             * If properties is null, invoked 'queryVertexProperties' to get the properties of the vertex
             */
            private Map<String, VertexProperty> getProperties() {
                if (this.properties == null) {
                    this.properties = queryVertexProperties(vertexId);
                }
                
                return this.properties;
            }
            
            @Override
            public int size() {
                return getProperties().size();
            }

            @Override
            public boolean isEmpty() {
                return getProperties().isEmpty();
            }

            @Override
            public boolean containsKey(Object key) {
                return getProperties().containsKey(key);
            }

            @Override
            public boolean containsValue(Object value) {
                return getProperties().containsValue(value);
            }

            @Override
            public VertexProperty get(Object key) {
                return getProperties().get(key);
            }

            @Override
            public VertexProperty put(String key, VertexProperty value) {
                return getProperties().put(key, value);
            }

            @Override
            public VertexProperty remove(Object key) {
                return getProperties().remove(key);
            }

            @Override
            public void putAll(Map<? extends String, ? extends VertexProperty> m) {
                getProperties().putAll(m);
            }

            @Override
            public void clear() {
                getProperties().clear();
            }

            @Override
            public Set<String> keySet() {
                return getProperties().keySet();
            }

            @Override
            public Collection<VertexProperty> values() {
                return getProperties().values();
            }

            @Override
            public Set<Entry<String, VertexProperty>> entrySet() {
                return getProperties().entrySet();
            }
        };
    }
}
