package org.mastik;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Utility function related to stream
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since 1/6/17
 */
public class StreamUtils {
    public static <T> Stream<T> toStream(Iterator<T> iterator) {
        Iterable<T> iterable = () -> iterator;
        return StreamSupport.stream(iterable.spliterator(), false);
    }
}
