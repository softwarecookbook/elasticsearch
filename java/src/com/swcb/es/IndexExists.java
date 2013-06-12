/* See the file "NOTICE" for the full license governing this code. */

package com.swcb.es;

import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;

/**
 * Checks to see if an index and a type exist
 */
public class IndexExists {

    /**
     * Check to see if the specified index exists
     * http://elasticsearch-users.115913.n3.nabble.com/JAVA-vs-REST-index-existence-check-td3918361.html
     * @param client
     * @param indexName
     * @return true if the index exists
     */
    public static boolean exists(Client client, String indexName) {
        IndicesExistsResponse response = client.admin().indices()
            .prepareExists(indexName)
            .execute().actionGet();
        boolean res = response.isExists();
        return res;
    }
    
    /**
     * Check to see if the specified index and type exists
     * http://elasticsearch-users.115913.n3.nabble.com/Java-API-to-get-a-mapping-td2988351.html
     * @param client
     * @param indexName
     * @return true if the index exists
     */
    public static boolean typeExists(Client client, String indexName, String indexType) {
        ClusterState cs = client.admin().cluster()
            .prepareState()
            .setFilterIndices(indexName)
            .execute().actionGet().getState();
        IndexMetaData imd = cs.getMetaData().index(indexName);
        if (imd != null) {
            MappingMetaData mdd = imd.mapping(indexType);
            if (mdd != null) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Example
     */
    public static void main(String[] args) {
        Client client = ESClient.MakeTransportClient("localhost", 9300);
        DeleteIndex.delete(client, "indextest");
        CreateIndex.create(client, "indextest");
        InsertDocument.insert(client, "indextest", "indextype1", "{\"f1\":\"dog\"}", "1");

        // Need to refresh index before testing
        RefreshIndex.refresh(client, "indextest");
        boolean res1 = IndexExists.exists(client, "indextest");
        boolean res2 = IndexExists.typeExists(client, "indextest", "indextype1");
        System.out.println("indextest exists? : " + res1 + ", indextype1 exists? : " + res2);
    }
}
