package org.mastik.structure.base;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.mastik.Backend;

import java.util.*;
import java.util.stream.Stream;

/**
 * A base class for all elements in Mastik
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since  1/6/17
 */
public class BaseMastikElement<TProperty extends Property> implements Element {
    private String id;
    private String label;
    private Map<String, TProperty> properties;
    private Backend backend;

    public BaseMastikElement(String id, String label, Map<String, TProperty> properties, Backend backend) {

        this.id = id;
        this.label = label;
        this.properties = properties;
        this.backend = backend;
    }

    /**
     * Gets the unique identifier for the graph {@code Element}.
     *
     * @return The id of the element
     */
    @Override
    public Object id() {
        return this.id;
    }

    /**
     * Gets the label for the graph {@code Element} which helps categorize it.
     *
     * @return The label of the element
     */
    @Override
    public String label() {
        return this.label;
    }

    /**
     * Get the graph that this element is within.
     *
     * @return the graph of this element
     */
    @Override
    public Graph graph() {
        throw new IllegalStateException("Graph object is not accessible via elements. Use backend() instead");
    }

    /**
     * Gets the backend, used for fetching element data from store
     */
    protected Backend backend() {
        return backend;
    }

    @Override
    public <V> Property<V> property(final String key) {
        TProperty property = this.properties.get(key);

        return property == null ? Property.empty() : property;
    }

    /**
     * Get the keys of the properties associated with this element.
     * The default implementation iterators the properties and stores the keys into a {@link HashSet}.
     *
     * @return The property key set
     */
    @Override
    public Set<String> keys() {
        return this.properties.keySet();
    }

    /**
     * Get an {@link java.util.stream.Stream} of properties.
     */
    public <V> Stream<? extends Property<V>> propertiesStream(String... propertyKeys) {
        if (propertyKeys.length == 0) {
            return (Stream) this.properties.values().stream();
        }

        return (Stream)Arrays.stream(propertyKeys)
                .map(this.properties::get)
                .filter(Objects::nonNull);
    }

    /**
     * Get an {@link Iterator} of properties.
     */
    @Override
    public <V> Iterator<? extends Property<V>> properties(String... propertyKeys) {
        return (Iterator)this.propertiesStream(propertyKeys).iterator();
    }

    /**
     * @return An exception indicating that this graph is readonly
     */
    public static IllegalStateException readonlyGraphException() {
        return new IllegalStateException("Mastik graph is readonly");
    }

    /**
     * @return An exception indicating that elements do not point to their parent elements
     */
    public static IllegalStateException noParentPointerException() {
        return new IllegalStateException("Mastik elements do not point to their parent element");
    }

    /**
     * Add or set a property value for the {@code Element} given its key.
     */
    @Override
    public <V> Property<V> property(String key, V value) {
        throw readonlyGraphException();
    }

    /**
     * Removes the {@code Element} from the graph.
     */
    @Override
    public void remove() {
        throw readonlyGraphException();
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

}
