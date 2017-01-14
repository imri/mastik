import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.search.sort.SortOrder;
import org.mastik.Backend;
import org.mastik.ElementCreator;
import org.mastik.ElementUtils;
import org.mastik.StreamUtils;
import org.mastik.query.PredicatesTree;
import org.mastik.query.Query;
import org.mastik.query.VertexQuery;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.*;
import java.util.stream.Stream;

/**
 * Elasticsearch Backend
 *
 * @author imriqwe (imriqwe@gmail.com)
 * @since 12/30/16
 */
public class ElasticsearchBackend implements Backend, AutoCloseable {

    private static final int DEFAULT_QUERY_LIMIT = 10000;
    private static final String EDGES_INDICES = "graph-edges";
    private static final String VERTICES_INDICES = "graph-vertices";
    private static final String EDGE_IN_VERTEX_PROPERTY = "inid";
    private static final String EDGE_OUT_VERTEX_PROPERTY = "outid";
    private static final String EDGE_LABEL_PROPERTY = "label";

    private static final Logger logger = LoggerFactory.getLogger(ElasticsearchBackend.class);

    private final Client client;
    private final ElementCreator elementCreator;
    private final ElasticsearchQueryCreator queryCreator;

    public ElasticsearchBackend(Collection<String> clusterHosts, ElementCreator elementCreator) {
        this.client = createClient(clusterHosts);
        this.elementCreator = elementCreator;
        this.queryCreator = new ElasticsearchQueryCreator(EDGE_LABEL_PROPERTY);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<Edge> queryVertex(VertexQuery vertexQuery) {
        PredicatesTree vertexPredicates = createVertexEdgesPredicates(vertexQuery.getVertexIds(), vertexQuery.getDirection());
        PredicatesTree mergedPredicates = PredicatesTree.and(vertexPredicates, vertexQuery.getPredicates()); // order is critical

        Query<Edge> query = new Query<>(Edge.class, mergedPredicates, vertexQuery.getLimit(), vertexQuery.getLabels(), vertexQuery.getOrders());
        return this.query(query);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<Vertex> getVerticesDeferred(Set<Object> vertexIds) {
        PredicatesTree idsPredicate = ElementUtils.createIdsPredicate(vertexIds);
        Query<Vertex> query = new Query<>(Vertex.class, idsPredicate, Query.noLimit(), Query.allLabels(), Query.noOrders());

        DeferredVerticesContainer container = new DeferredVerticesContainer(query, this);

        return vertexIds.stream()
                .map(vertexId -> elementCreator.createDeferredVertex(
                        vertexId.toString(), container.makeVertexPropertiesMap(vertexId), this));
    }

    /**
     * Given a {@link Query}, converts it to a {@link SearchRequestBuilder},
     * sends it to Elasticsearch, then tests the results against the query
     *
     * @return Collection of result elements ({@link Vertex}s or {@link Edge}s)
     */
    @Override
    public <E extends Element> Stream<E> query(Query<E> query) {
        logger.debug("Running query: {}", query);

        try {
            return this.search(query.getReturnType(), this.createSearchRequest(query))
                    .filter(element -> query.test(element, query.getPredicates()));
        } catch (IOException e) {
            throw new RuntimeException("Query failed due to an inner exception", e);
        }
    }

    /**
     * Given vertex-ids and any direction,
     * creates predicates matching the edge documents in-id/out-id
     */
    private PredicatesTree createVertexEdgesPredicates(Set<Object> vertexIds, Direction direction) {
        if (direction == Direction.BOTH) {
            return PredicatesTree.or(
                    createVertexInOutPredicates(vertexIds, Direction.IN),
                    createVertexInOutPredicates(vertexIds, Direction.OUT));
        }

        return createVertexInOutPredicates(vertexIds, direction);
    }

    /**
     * Given vertex-ids and a direction which is in/out,
     * creates predicates matching the edge documents in-id/out-id
     */
    private PredicatesTree createVertexInOutPredicates(Set<Object> vertexIds, Direction direction) {
        String propertyName = direction == Direction.IN ? EDGE_IN_VERTEX_PROPERTY : EDGE_OUT_VERTEX_PROPERTY;
        HasContainer idsPredicate = new HasContainer(propertyName, P.within(vertexIds));

        return PredicatesTree.createFromPredicates(idsPredicate);
    }

    /**
     * Creates a {@link SearchRequestBuilder} from a given {@link Query}
     */
    private <E extends Element> SearchRequestBuilder createSearchRequest(Query<E> query) {
        int limit = query.getLimit() >= 0 ? query.getLimit() : DEFAULT_QUERY_LIMIT;

        SearchRequestBuilder searchRequestBuilder = client.prepareSearch(determineIndex(query))
                .setQuery(this.queryCreator.createFromPredicates(query.getPredicates()))
                .setSize(limit)
                .setFetchSource(true);// @todo: define specifically properties to fetch from source

        if (query.hasOrders()) {
            query.getOrders().forEach(order -> {
                Order orderValue = order.getValue1();
                switch (orderValue) {
                    case decr:
                        searchRequestBuilder.addSort(order.getValue0(), SortOrder.DESC);
                        break;
                    case incr:
                        searchRequestBuilder.addSort(order.getValue0(), SortOrder.ASC);
                        break;
                    case shuffle:
                        break;
                }
            });
        }

        // @todo: add routing

        return searchRequestBuilder;
    }

    /**
     * Given a search-request, performs the search and returns {@link Stream} of elements
     */
    private <E extends Element> Stream<E> search(Class<E> returnType, SearchRequestBuilder search) throws IOException {
        SearchResponse searchResponse = search.execute().actionGet();

        if (searchResponse.status().getStatus() != 200) {
            logger.warn("Request {} got {} response status, returned empty stream", search, searchResponse.status());

            return Stream.empty();
        }

        return StreamUtils.toStream(searchResponse.getHits().iterator())
                .map(item -> createElement(returnType, item.getId(), item.getSource()));
    }

    /**
     * Given element type, id and properties,
     * creates an instance of {@link Vertex} or {@link Edge} accordingly
     *
     * @return New instance of a Vertex of Edge, according to the given element type
     */
    private <E extends Element> E createElement(Class<E> elementType, String elementId, Map<String, Object> properties) {
        if (ElementUtils.isEdge(elementType)) {
            String outVertexId = properties.get(EDGE_OUT_VERTEX_PROPERTY).toString();
            String inVertexId = properties.get(EDGE_IN_VERTEX_PROPERTY).toString();
            String label = properties.getOrDefault(EDGE_LABEL_PROPERTY, Edge.DEFAULT_LABEL).toString();

            Vertex outVertex = this.elementCreator.createVertexFromId(outVertexId, this);
            Vertex inVertex = this.elementCreator.createVertexFromId(inVertexId, this);

            return (E) this.elementCreator.createEdge(elementId, label, properties, outVertex, inVertex, this);
        } else if (ElementUtils.isVertex(elementType)) {
            return (E) this.elementCreator.createVertex(elementId, properties, this);
        }

        throw new IllegalStateException(String.format("Cannot create an element of type '%s'", elementType));
    }

    /**
     * Given a query, returns the index it should be queried from
     */
    private <E extends Element> String determineIndex(Query<E> query) {
        if (ElementUtils.isEdge(query.getReturnType())) {
            return EDGES_INDICES;
        } else if (ElementUtils.isVertex(query.getReturnType())) {
            return VERTICES_INDICES;
        }

        throw new IllegalStateException(String.format("Cannot execute query with '%s' return type", query.getReturnType()));
    }


    /**
     * Create an Elasticsearch client given cluster hosts
     * @param nodes Collection of HOST:PORT strings, representing nodes in the cluster
     * @return New instance of a client
     */
    private Client createClient(Collection<String> nodes) {
        TransportClient client = TransportClient.builder().build();

        for (String host : nodes) {
            try {
                URI uri = new URI("http://" + host);
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(uri.getHost()), uri.getPort()));
            } catch (URISyntaxException | UnknownHostException e) {
                logger.warn("Failed to add Elasticsearch node '{}' to client due to inner exception: {}", host, e);
            }
        }

        return client;
    }

    @Override
    public void close() throws Exception {
        this.client.close();
    }
}
