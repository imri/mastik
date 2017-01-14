import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.elasticsearch.action.search.MultiSearchRequestBuilder;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
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
 * @since  12/30/16
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

    private Client createClient(Collection<String> clusterHosts) {
        TransportClient client = TransportClient.builder().build();

        for (String host : clusterHosts) {
            try {
                URI uri = new URI("http://" + host);
                client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(uri.getHost()), uri.getPort()));
            } catch (URISyntaxException|UnknownHostException e) {
                logger.warn("Failed to add Elasticsearch node '{}' to client due to inner exception: {}", host, e);
            }
        }

        return client;
    }

    @Override
    public Stream<Edge> queryVertex(VertexQuery query) {
        // optimization for vertex out edges queries
        // @todo: if it is an BOTH query, should we optimize it too?
//        if (query.getDirection() == Direction.OUT) {
//            return searchVerticesOutEdges(query);
//        }

        PredicatesTree vertexPredicates = makeVertexEdgesPredicates(query.getVertexIds(), query.getDirection());
        PredicatesTree mergedPredicates = PredicatesTree.and(vertexPredicates, query.getPredicates()); // order is critical

        Query<Edge> completeQuery = new Query<>(Edge.class, mergedPredicates, query.getLimit(), query.getLabels(), query.getOrders());
        return this.query(completeQuery);
    }

    @Override
    public Stream<Vertex> getVerticesDeferred(Set<Object> verticesIds) {
        PredicatesTree idsPredicate = ElementUtils.createIdsPredicate(verticesIds);
        Query<Vertex> query = new Query<>(Vertex.class, idsPredicate, Query.noLimit(), Query.allLabels(), Query.noOrders());

        DeferredVerticesContainer container = new DeferredVerticesContainer(query, this);

        return verticesIds.stream()
                .map(vertexId -> elementCreator.createDeferredVertex((String)vertexId, container.makeVertexPropertiesMap(vertexId), this));
    }

    @Override
    public <E extends Element> Stream<E> query(Query<E> query) {
        logger.info("Running query: {}", query);

        SearchRequestBuilder searchRequest = this.createSearchRequest(query);

        try {
            return this.search(query.getReturnType(), Collections.singletonList(searchRequest))
                    .filter(element -> query.test(element, query.getPredicates()));
        } catch (IOException e) {
            throw new RuntimeException("Search failed due to inner exception", e);
        }
    }

    private Iterator<Edge> searchVerticesOutEdges(VertexQuery query) {
//        List<Search> searches = query
//                .getVertexIds()
//                .stream()
//                .map(vertex -> createVertexOutEdgesSearch(query, queryBuilder, vertex.id()))
//                .collect(Collectors.toList());
//
//        return null;
        throw new IllegalStateException("Not implemented yet");
    }

    private PredicatesTree makeVertexEdgesPredicates(Set<Object> vertexIds, Direction direction) {
        if (direction == Direction.BOTH) {
            return PredicatesTree.or(
                    makeVerticesPredicates(vertexIds, Direction.IN),
                    makeVerticesPredicates(vertexIds, Direction.OUT));
        }

        return makeVerticesPredicates(vertexIds, direction);
    }

    private PredicatesTree makeVerticesPredicates(Set<Object> vertexIds, Direction direction) {
        String propertyName = direction == Direction.IN ? EDGE_IN_VERTEX_PROPERTY : EDGE_OUT_VERTEX_PROPERTY;
        HasContainer idsPredicate = new HasContainer(propertyName, P.within(vertexIds));

        return PredicatesTree.or(idsPredicate);
    }

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

    private <E extends Element> String determineIndex(Query<E> query) {
        if (query.getReturnType().isAssignableFrom(Edge.class)) {
            return EDGES_INDICES;
        } else if (query.getReturnType().isAssignableFrom(Vertex.class)) {
            return VERTICES_INDICES;
        }

        throw new IllegalStateException(String.format("Cannot execute query with '%s' return type", query.getReturnType()));
    }

    private <E extends Element> Stream<E> search(Class<E> returnType, List<SearchRequestBuilder> searches) throws IOException {
        MultiSearchRequestBuilder multiSearch = this.client.prepareMultiSearch();
        searches.forEach(multiSearch::add);

        MultiSearchResponse response = multiSearch.execute().actionGet();

        return Arrays.stream(response.getResponses())
                .filter(item -> !item.isFailure())
                .flatMap(item -> StreamUtils.toStream(item.getResponse().getHits().iterator()))
                .map(item -> createElement(returnType, item.getId(), item.getSource()));
    }

    private <E extends Element> E createElement(Class<E> elementType, String elementId, Map<String, Object> properties) {
        if (ElementUtils.isEdge(elementType)) {
            Vertex outVertex = this.elementCreator.createVertexFromId((String) properties.get(EDGE_OUT_VERTEX_PROPERTY), this);
            Vertex inVertex = this.elementCreator.createVertexFromId((String) properties.get(EDGE_IN_VERTEX_PROPERTY), this);
            String label = (String) properties.getOrDefault(EDGE_LABEL_PROPERTY, Edge.DEFAULT_LABEL);

            return (E) this.elementCreator.createEdge(elementId, label, properties, outVertex, inVertex, this);
        } else if (ElementUtils.isVertex(elementType)) {
            return (E) this.elementCreator.createVertex(elementId, properties, this);
        }

        throw new IllegalStateException(String.format("Cannot create an element of type '%s'", elementType));
    }

    @Override
    public void close() throws Exception {
        this.client.close();
    }
}
