package org.neo4j.plugins.elasticsearch;

import org.neo4j.configuration.Description;
import org.neo4j.graphdb.config.Setting;

import static org.neo4j.kernel.configuration.Settings.*;

@Description("settings for neo4j elasticsearch plugin")
public final class ElasticSearchSettings {

    /**
     * The host of elasticsearch cluster.
     */
    public static final Setting<String> HOST = setting("neo4j.plugin.elasticsearch.host", STRING, "");

    /**
     * The elasticsearch index name to store data.
     */
    public static final Setting<String> INDEX_NAME = setting("neo4j.plugin.elasticsearch.indexName", STRING,
            "index_default");

    /**
     * The number of shards of the specific index, default 5.
     */
    public static final Setting<Integer> NUMBER_OF_SHARDS = setting("neo4j.plugin.elasticsearch.numberOfShards",
            INTEGER, "5");

    /**
     * The number of replicas of the specific index, default 1.
     */
    public static final Setting<Integer> NUMBER_OF_REPLICAS = setting("neo4j.plugin.elasticsearch.numberOfReplicas",
            INTEGER, "1");

    /**
     * Whether to sync nodes or not, default true.
     */
    public static final Setting<Boolean> SYNC_NODES = setting("neo4j.plugin.elasticsearch.syncNodes", BOOLEAN, TRUE);

    /**
     * Whether to sync relationships or not, default false.
     */
    public static final Setting<Boolean> SYNC_RELATIONSHIPS = setting("neo4j.plugin.elasticsearch.syncRelationships",
            BOOLEAN, FALSE);

    /**
     * Elasticsearch discovery mode.
     */
    public static final Setting<Boolean> DISCOVERY = setting("neo4j.plugin.elasticsearch.discovery", BOOLEAN, FALSE);

    /**
     * Transfer data asynchronously.
     */
    public static final Setting<Boolean> EXECUTE_ASYNC = setting("neo4j.plugin.elasticsearch.executeAsync", BOOLEAN,
            TRUE);
}
