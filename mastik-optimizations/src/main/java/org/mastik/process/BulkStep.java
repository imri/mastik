package org.mastik.process;

import com.google.common.collect.Iterators;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.util.iterator.EmptyIterator;
import org.mastik.StreamUtils;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

public abstract class BulkStep<S, E> extends AbstractStep<S, E> {
    /**
     * Default bulk size
     * todo: make this configurable
     */
    protected final int bulkSize = 1000;

    /**
     * Results to be returned each time 'processNextStart' is invoked
     */
    private Iterator<Traverser.Admin<E>> results = EmptyIterator.instance();

    public BulkStep(Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    protected Traverser.Admin<E> processNextStart() throws NoSuchElementException {
        if (!results.hasNext() && starts.hasNext()) {
            results = process();
        }

        if (results.hasNext()) {
            return results.next();
        }

        throw FastNoSuchElementException.instance();
    }

    /**
     * Partitions 'starts' to bulks in 'bulkSize', passing each to 'process'
     * @return
     */
    private Iterator<Traverser.Admin<E>> process() {
        return StreamUtils.toStream(Iterators.partition(this.starts, this.bulkSize))
                .flatMap(this::process)
                .iterator();
    }

    /**
     * Builds a {@link Stream} of result Traversers from a given Traversers bulk
     * @param traversers Input traversers to process
     * @return Output traversers to return
     */
    protected abstract Stream<Traverser.Admin<E>> process(List<Traverser.Admin<S>> traversers);

    @Override
    public void reset() {
        super.reset();

        this.results = EmptyIterator.instance();
    }
}
