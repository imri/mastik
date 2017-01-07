package org.mastik;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

/**
 * Traversal Strategy that uses a Backend
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since 1/6/17
 */
public interface Strategy {
    /**
     * Apply the strategy to a traversal
     * @param traversal Traversal to apply the strategy to
     * @param backend Backend to use for elements retrieval
     */
    void apply(Traversal.Admin<?, ?> traversal, Backend backend);
}
