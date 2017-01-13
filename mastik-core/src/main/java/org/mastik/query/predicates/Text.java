package org.mastik.query.predicates;

import org.apache.tinkerpop.gremlin.process.traversal.P;

import java.util.function.BiPredicate;

/**
 * Text functions and predicates
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since 1/13/17
 */
public class Text {
    public static <V> P<V> like(final V value) {
        return new P(TextPredicate.LIKE, value);
    }

    public static <V> P<V> regex(final V value) {
        return new P(TextPredicate.REGEX, value);
    }

    public static <V> P<V> prefix(final V value) {
        return new P(TextPredicate.PREFIX, value);
    }

    public enum TextPredicate implements BiPredicate<Object, Object> {
        PREFIX {
            @Override
            public boolean test(final Object first, final Object second) {
                return first.toString().startsWith(second.toString());
            }

            /**
             * The negative of {@code LIKE} is {@link #UNLIKE}.
             */
            @Override
            public TextPredicate negate() {
                return UNREGEX;
            }
        },
        UNPREFIX {
            @Override
            public boolean test(final Object first, final Object second) {
                return !negate().test(first, second);
            }

            /**
             * The negative of {@code LIKE} is {@link #UNLIKE}.
             */
            @Override
            public TextPredicate negate() {
                return PREFIX;
            }
        },
        LIKE {
            @Override
            public boolean test(final Object first, final Object second) {
                return first.toString().matches(second.toString().replace("?", ".?").replace("*", ".*?"));
            }

            @Override
            public TextPredicate negate() {
                return UNLIKE;
            }
        },
        UNLIKE {
            @Override
            public boolean test(final Object first, final Object second) {
                return !negate().test(first, second);
            }

            @Override
            public TextPredicate negate() {
                return LIKE;
            }
        },
        REGEX {
            @Override
            public boolean test(final Object first, final Object second) {
                return first.toString().matches(second.toString());
            }

            @Override
            public TextPredicate negate() {
                return UNREGEX;
            }
        },
        UNREGEX {
            @Override
            public boolean test(final Object first, final Object second) {
                return !negate().test(first, second);
            }

            @Override
            public TextPredicate negate() {
                return REGEX;
            }
        }
    }
}
