package org.neo4j.plugins.elasticsearch;

import io.searchbox.action.BulkableAction;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.client.JestResultHandler;
import io.searchbox.core.Bulk;
import io.searchbox.core.Delete;
import io.searchbox.core.DocumentResult;
import io.searchbox.core.Index;
import org.neo4j.graphdb.Entity;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.event.LabelEntry;
import org.neo4j.graphdb.event.PropertyEntry;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ElasticSearchEventHandler implements TransactionEventHandler<Collection<BulkableAction<DocumentResult>>>,
        JestResultHandler<JestResult> {

    private final static Logger logger = Logger.getLogger(ElasticSearchEventHandler.class.getName());

    /**
     * The key of neo4j entity id that stores in elasticsearch.
     */
    private static final String ID = "id";

    /**
     * The key of neo4j node labels that stores in elasticsearch.
     */
    private static final String LABELS = "labels";

    /**
     * The key of neo4j relationship type that stores in elasticsearch.
     */
    private static final String TYPE = "type";

    /**
     * The key of neo4j relationship startNodeId that stores in elasticsearch.
     */
    private static final String START_NODE_ID = "startNodeId";

    /**
     * The key of neo4j relationship endNodeId that stores in elasticsearch.
     */
    private static final String END_NODE_ID = "endNodeId";

    /**
     * The key of neo4j entity properties that stores in elasticsearch.
     */
    private static final String PROPERTIES = "properties";

    /**
     * The prefix of elasticsearch type that stores neo4j nodes.
     */
    private static final String PREFIX_TYPE_NODE = "type_node_";

    /**
     * The prefix of elasticsearch type that stores neo4j relationships.
     */
    private static final String PREFIX_TYPE_RELATIONSHIP = "type_relationship_";

    private final JestClient jestClient;

    private final String indexName;

    private final String typeNode;

    private final String typeRelationship;

    private final boolean syncNodes;

    private final boolean syncRelationships;

    private final boolean executeAsync;

    private ElasticSearchEventHandler(Builder builder) {
        this.jestClient = builder.jestClient;
        this.indexName = builder.indexName;
        this.syncNodes = builder.syncNodes;
        this.syncRelationships = builder.syncRelationships;
        this.executeAsync = builder.executeAsync;
        this.typeNode = PREFIX_TYPE_NODE + indexName;
        this.typeRelationship = PREFIX_TYPE_RELATIONSHIP + indexName;
    }

    public static class Builder {

        private JestClient jestClient;

        private String indexName;

        private boolean syncNodes;

        private boolean syncRelationships;

        private boolean executeAsync;

        public Builder() {
        }

        public Builder jestClient(JestClient jestClient) {
            this.jestClient = jestClient;
            return this;
        }

        public Builder indexName(String indexName) {
            this.indexName = indexName;
            return this;
        }

        public Builder syncNodes(boolean syncNodes) {
            this.syncNodes = syncNodes;
            return this;
        }

        public Builder syncRelationships(boolean syncRelationships) {
            this.syncRelationships = syncRelationships;
            return this;
        }

        public Builder executeAsync(boolean executeAsync) {
            this.executeAsync = executeAsync;
            return this;
        }

        public ElasticSearchEventHandler build() {
            return new ElasticSearchEventHandler(this);
        }
    }

    @Override
    public void completed(JestResult result) {
        if (result.isSucceeded() && result.getErrorMessage() == null) {
            logger.fine("data transfer completed");
        } else {
            logger.severe("data transfer error: " + result.getErrorMessage());
        }
    }

    @Override
    public void failed(Exception ex) {
        logger.log(Level.WARNING, "data transfer failed", ex);
    }

    @Override
    public Collection<BulkableAction<DocumentResult>> beforeCommit(TransactionData data) throws Exception {
        Map<SyncDataKey, BulkableAction<DocumentResult>> actions = new LinkedHashMap<>();
        if (syncNodes) {
            // all changed nodes
            collectChangedNodes(actions, data);
        }

        if (syncRelationships) {
            // all changed relationships
            collectChangedRelations(actions, data);
        }

        return actions.isEmpty() ? Collections.emptyList() : actions.values();
    }

    @Override
    public void afterCommit(TransactionData data, Collection<BulkableAction<DocumentResult>> state) {
        if (state.isEmpty()) {
            return;
        }

        try {
            Bulk bulk = new Bulk.Builder().addAction(state).build();
            if (executeAsync) {
                jestClient.executeAsync(bulk, this);
            } else {
                jestClient.execute(bulk);
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "data transfer execution error after commit", e);
        }
    }

    @Override
    public void afterRollback(TransactionData data, Collection<BulkableAction<DocumentResult>> state) {
    }

    private void collectChangedNodes(Map<SyncDataKey, BulkableAction<DocumentResult>> actions, TransactionData data) {
        // created nodes
        for (Node node : data.createdNodes()) {
            String id = id(node);
            actions.put(key(typeNode, id), indexRequest(id, node));
        }

        // deleted nodes
        for (Node node : data.deletedNodes()) {
            String id = id(node);
            actions.put(key(typeNode, id), deleteRequest(id, node));
        }

        // assigned labels
        for (LabelEntry labelEntry : data.assignedLabels()) {
            Node node = labelEntry.node();
            String id = id(node);
            if (data.isDeleted(labelEntry.node())) {
                actions.put(key(typeNode, id), deleteRequest(id, node));
            } else {
                actions.put(key(typeNode, id), indexRequest(id, labelEntry.node()));
            }
        }

        // removed labels
        for (LabelEntry labelEntry : data.removedLabels()) {
            Node node = labelEntry.node();
            String id = id(node);
            actions.put(key(typeNode, id), deleteRequest(id, node));
        }

        // assigned node properties
        for (PropertyEntry<Node> propEntry : data.assignedNodeProperties()) {
            Node node = propEntry.entity();
            String id = id(node);
            actions.put(key(typeNode, id), indexRequest(id, node));
        }

        // removed node properties
        for (PropertyEntry<Node> propEntry : data.removedNodeProperties()) {
            Node node = propEntry.entity();
            String id = id(node);
            if (data.isDeleted(node)) {
                actions.put(key(typeNode, id), deleteRequest(id, node));
            } else {
                actions.put(key(typeNode, id), indexRequest(id, node));
            }
        }
    }

    private void collectChangedRelations(Map<SyncDataKey, BulkableAction<DocumentResult>> actions,
            TransactionData data) {
        // created relationships
        for (Relationship relationship : data.createdRelationships()) {
            String id = id(relationship);
            actions.put(key(typeRelationship, id), indexRequest(id, relationship));
        }

        // deleted relationships
        for (Relationship relationship : data.deletedRelationships()) {
            String id = id(relationship);
            actions.put(key(typeRelationship, id), deleteRequest(id, relationship));
        }

        // assigned relationship properties
        for (PropertyEntry<Relationship> propEntry : data.assignedRelationshipProperties()) {
            Relationship relationship = propEntry.entity();
            String id = id(relationship);
            actions.put(key(typeRelationship, id), indexRequest(id, relationship));
        }

        // removed relationship properties
        for (PropertyEntry<Relationship> propEntry : data.removedRelationshipProperties()) {
            Relationship relationship = propEntry.entity();
            String id = id(relationship);
            if (data.isDeleted(relationship)) {
                actions.put(key(typeRelationship, id), deleteRequest(id, relationship));
            } else {
                actions.put(key(typeRelationship, id), indexRequest(id, relationship));
            }
        }
    }

    private SyncDataKey key(String type, String id) {
        return new SyncDataKey(indexName, type, id);
    }

    private BulkableAction<DocumentResult> indexRequest(String id, Entity entity) {
        Index.Builder builder = new Index.Builder(properties(id, entity)).index(indexName).id(id);
        if (entity instanceof Node) {
            builder.type(typeNode);
        } else if (entity instanceof Relationship) {
            builder.type(typeRelationship);
        }
        return builder.build();
    }

    private BulkableAction<DocumentResult> deleteRequest(String id, Entity entity) {
        Delete.Builder builder = new Delete.Builder(id).index(indexName);
        if (entity instanceof Node) {
            builder.type(typeNode);
        } else if (entity instanceof Relationship) {
            builder.type(typeRelationship);
        }
        return builder.build();
    }

    private String id(Entity entity) {
        return String.valueOf(entity.getId());
    }

    private Map<String, Object> properties(String id, Entity entity) {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put(ID, id);
        props.put(PROPERTIES, entity.getAllProperties());
        if (entity instanceof Node) {
            // node labels
            props.put(LABELS, labels(((Node) entity).getLabels()));
        } else if (entity instanceof Relationship) {
            // relationship type
            props.put(TYPE, ((Relationship) entity).getType().name());
            // relationship startNodeId
            props.put(START_NODE_ID, String.valueOf(((Relationship) entity).getStartNodeId()));
            // relationship endNodeId
            props.put(END_NODE_ID, String.valueOf(((Relationship) entity).getEndNodeId()));
        }
        return props;
    }

    private List<String> labels(Iterable<Label> labels) {
        List<String> list = new LinkedList<>();
        for (Label label : labels) {
            list.add(label.name());
        }
        return list;
    }

    /**
     * The key of the map which stores changed data before commit.
     * This is used to avoid duplicate data transformation.
     * Only data with different index, type and id can be transferred
     * to elasticsearch.
     */
    private static class SyncDataKey {

        private final String index;

        private final String type;

        private final String id;

        public SyncDataKey(String index, String type, String id) {
            this.index = index;
            this.type = type;
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SyncDataKey that = (SyncDataKey) o;
            if (!index.equals(that.index)) {
                return false;
            }
            if (!type.equals(that.type)) {
                return false;
            }
            return id.equals(that.id);
        }

        @Override
        public int hashCode() {
            int result = index.hashCode();
            result = 31 * result + type.hashCode();
            result = 31 * result + ((id == null) ? 0 : id.hashCode());
            return result;
        }
    }
}
