package org.mastik.structure;

import com.google.common.base.Joiner;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.*;
import org.mastik.structure.base.BaseMastikElement;

import java.util.Collections;

/**
 * A standard implementation of a property
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since  1/6/17
 */
public class MastikProperty<E extends Element, V> extends BaseMastikElement implements Property<V> {
    private final String key;
    private V value;

    public MastikProperty(String key, V value) {
        super(generateId(key, value), null, Collections.emptyMap(), null);
        ElementHelper.validateProperty(key, value);

        this.key = key;
        this.value = value;
    }

    /**
     * Given a key and value, concatenates them
     */
    public static <E extends Element, V> String generateId(String key, V value) {
        return Joiner.on("-").join(key, value);
    }

    /**
     * Get the element that this property is associated with.
     *
     * @return The element associated with this property (i.e. {@link Vertex}, {@link Edge}, or {@link VertexProperty}).
     */
    @Override
    public E element() {
        throw BaseMastikElement.noParentPointerException();
    }

    /**
     * The key of the property.
     *
     * @return The property key
     */
    @Override
    public String key() {
        return this.key;
    }

    /**
     * The value of the property.
     *
     * @return The property value
     */
    @Override
    public V value() {
        return this.value;
    }

    /**
     * Whether the property is emptyTree or not.
     *
     * @return True if the property exists, else false
     */
    @Override
    public boolean isPresent() {
        return this.value != null;
    }

    /**
     * Remove the property from the associated element.
     */
    @Override
    public void remove() {
        throw BaseMastikElement.readonlyGraphException();
    }

    public String toString() {
        return StringFactory.propertyString(this);
    }

    public boolean equals(final Object object) {
        return ElementHelper.areEqual((Property)this, object);
    }

    public int hashCode() {
        return ElementHelper.hashCode((Property)this);
    }
}
