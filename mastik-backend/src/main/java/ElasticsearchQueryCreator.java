import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;
import org.elasticsearch.index.query.*;
import org.mastik.StreamUtils;
import org.mastik.query.PredicatesTree;
import org.mastik.query.predicates.Text;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

/**
 * Creates Elasticsearch query from predicates
 * todo: add Date and Exists predicates
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since 1/13/17
 */
public class ElasticsearchQueryCreator {
    private String edgeLabelKey;

    public ElasticsearchQueryCreator(String edgeLabelKey) {
        this.edgeLabelKey = edgeLabelKey;
    }

    /**
     * Converts a {@link PredicatesTree} to an Elasticsearch query
     */
    public QueryBuilder createFromPredicates(PredicatesTree predicatesTree) {
        List<QueryBuilder> filters = Lists.newArrayList();

        if (predicatesTree.hasPredicates()) {
            predicatesTree.predicates().stream()
                    .map(this::createFromHasContainer)
                    .forEach(filters::add);
        }

        if (predicatesTree.hasChildren()) {
            predicatesTree.children().stream()
                    .map(this::createFromPredicates)
                    .forEach(filters::add);
        }

        if (filters.isEmpty()) {
            return QueryBuilders.matchAllQuery();
        }

        if (filters.size() == 1) {
            return filters.iterator().next();
        }

        BoolQueryBuilder query = QueryBuilders.boolQuery();

        if (predicatesTree.isAnd()) {
            filters.forEach(query::must);
        } else if (predicatesTree.isOr()) {
            filters.forEach(query::should);
        } else {
            throw new IllegalStateException("Predicates tree is neither 'and' nor 'or'");
        }

        return QueryBuilders.constantScoreQuery(query);
    }

    /**
     * Converts a {@link HasContainer} to an Elasticsearch query
     */
    private QueryBuilder createFromHasContainer(HasContainer hasContainer) {
        String key = hasContainer.getKey();
        P predicate = hasContainer.getPredicate();
        Object value = predicate.getValue();
        BiPredicate biPredicate = predicate.getBiPredicate();

        if (key.equals(T.id.getAccessor())) {
            if (value instanceof Iterable) {
                return createIdsFilter(getStringIds((Iterable) value));
            }

            return createIdFilter(value.toString());
        }

        if (key.equals(T.label.getAccessor())) {
            key = this.edgeLabelKey;
        }

        if (biPredicate != null) {
            if (biPredicate instanceof Compare) {
                return createCompareFilter((Compare)biPredicate, key, value);
            }

            if (biPredicate instanceof Contains) {
                return createContainsFilter((Contains)biPredicate, key, value);
            }

            if (biPredicate instanceof Text.TextPredicate) {
                return createTextFilter((Text.TextPredicate)biPredicate, key, value.toString());
            }

            throw unsupportedPredicateException(predicate);
        }

        throw unsupportedPredicateException(predicate);
    }

    /**
     * Creates a text filter given a predicate, key and value
     */
    private static QueryBuilder createTextFilter(Text.TextPredicate predicate, String key, String value) {
        switch (predicate) {
            case LIKE:
                return QueryBuilders.wildcardQuery(key, value);
            case UNLIKE:
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.wildcardQuery(key, value));
            case PREFIX:
                return QueryBuilders.prefixQuery(key, value);
            case UNPREFIX:
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.prefixQuery(key, value));
            case REGEX:
                return QueryBuilders.regexpQuery(key, value);
            case UNREGEX:
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.regexpQuery(key, value));
        }

        throw unsupportedPredicateException(predicate);
    }

    /**
     * Runs 'toString' per each item of the given collection, returns a set with the results
     *
     * @param ids Collection of input ids
     * @return Set of string ids
     */
    private Set<String> getStringIds(Iterable ids) {
        return StreamUtils.toStream(((Iterable<Object>) ids).iterator())
                .map(Object::toString).collect(Collectors.toSet());
    }

    /**
     * Creates a filter for multiple ids
     */
    private static QueryBuilder createIdsFilter(Collection<String> ids) {
        return QueryBuilders.idsQuery().addIds(ids);
    }

    /**
     * Creates a filter for a single id
     */
    private static QueryBuilder createIdFilter(String id) {
        return QueryBuilders.idsQuery().addIds(id);
    }

    /**
     * Converts a {@link Compare} predicate to an Elasticsearch filter
     */
    private static QueryBuilder createCompareFilter(Compare predicate, String key, Object value) {
        switch (predicate) {
            case eq:
                return QueryBuilders.termQuery(key, value);
            case neq:
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(key, value));
            case gt:
                return QueryBuilders.rangeQuery(key).gt(value);
            case gte:
                return QueryBuilders.rangeQuery(key).gte(value);
            case lt:
                return QueryBuilders.rangeQuery(key).lt(value);
            case lte:
                return QueryBuilders.rangeQuery(key).lte(value);
        }

        throw unsupportedPredicateException(predicate);
    }

    /**
     * Converts a {@link Contains} predicate to an Elasticsearch filter
     */
    private static QueryBuilder createContainsFilter(Contains predicate, String key, Object value) {
        switch (predicate) {
            case without:
                return QueryBuilders.boolQuery().mustNot(QueryBuilders.existsQuery(key));
            case within:
                if (value == null) {
                    return QueryBuilders.existsQuery(key);
                }

                if (value instanceof Collection) {
                    return QueryBuilders.termsQuery(key, (Collection<?>) value);
                }

                if (value.getClass().isArray()) {
                    return QueryBuilders.termsQuery(key, (Object[]) value);
                }

                return QueryBuilders.termsQuery(key, value);
        }

        throw unsupportedPredicateException(predicate);
    }

    /**
     * Returns an exception indicating predicate is not supported
     */
    private static IllegalStateException unsupportedPredicateException(Object predicate) {
        return new IllegalStateException(String.format("Predicate '%s' not supported by Mastik", predicate));
    }
}
