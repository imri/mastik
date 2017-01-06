package org.mastik.structure;

import org.apache.tinkerpop.gremlin.structure.*;
import org.mastik.structure.base.BaseMastikVertex;

/**
 * A standard implementation of a vertex property
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since  1/6/17
 */
public class MastikVertexProperty<V> extends MastikProperty<BaseMastikVertex,V> implements VertexProperty<V> {
    public MastikVertexProperty(final String key, final V value) {
        super(key, value);
    }
}