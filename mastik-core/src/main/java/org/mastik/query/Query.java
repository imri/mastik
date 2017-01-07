package org.mastik.query;

import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.javatuples.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Set;

/**
 * A query with property keys, predicates, limit and order
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since  1/6/17
 */
public class Query<E extends Element> extends PredicatesQuery<E> {
    /**
     * Use this value to indicate this query is not limited in results number
     */
    public static int noLimit() { return -1; }

    /**
     * Use this object to indicate that this query is not ordered
     */
    public static List<Pair<String, Order>> noOrders() { return Collections.emptyList(); }

    /**
     * Use this object to indicate this query should return all properties
     */
    public static Set<String> allProperties() { return Collections.emptySet(); }

    private final Class<E> returnType;
    private final int limit;
    private final Set<String> propertyKeys;
    private final List<Pair<String, Order>> orders;

    public Query(Class<E> returnType, PredicatesTree predicatesContainer, int limit, Set<String> propertyKeys, List<Pair<String, Order>> orders) {
        super(predicatesContainer);

        this.returnType = returnType;
        this.limit = limit;
        this.propertyKeys = propertyKeys;
        this.orders = orders;
    }

    /**
     * Returns the return type of this query
     */
    public Class<E> getReturnType(){
        return returnType;
    }

    /**
     * Returns the property keys for this query results
     */
    public Set<String> getPropertyKeys() {
        return this.propertyKeys;
    }

    /**
     * Returns the limit of this query
     */
    public int getLimit(){
        return limit;
    }

    /**
     * Returns the orders of this query
     */
    public List<Pair<String, Order>> getOrders() {
        return orders;
    }

    /**
     * Whether orders were supplied for this query
     */
    public boolean hasOrders() {
        return getOrders() != null;
    }

    @Override
    public String toString() {
        return String.format("Query{returnType=%s, limit=%s}", this.returnType, this.limit);
    }
}
