package com.swcb.es;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;

/**
 * Count the documents in an index and an index with type
 */
public class CountDocuments {

    /**
     * Count all docs in index for all types
     * @param indexName
     * @return number of docs found in index  (WHAT HAPPENS IF INVALID?)
     */
    public static long count(Client esClient, String indexName) {
        SearchResponse response = esClient.prepareSearch(indexName)
            .setQuery(QueryBuilders.matchAllQuery())
            .execute()
            .actionGet();
        long numResults = response.getHits().getTotalHits();  //same as .totalHits() ?
        return numResults;
    }
    
    /**
     * Count all docs in index for this type
     * Note: the index should be refreshed before using - no API available to do this.
     * @param indexName
     * @param indexType
     * @return number of docs found in index  (WHAT HAPPENS IF INVALID?)
     */
    public static long count(Client esClient, String indexName, String indexType) {
        SearchResponse response = esClient.prepareSearch(indexName)
            .setQuery(QueryBuilders.matchAllQuery())
            .setTypes(indexType)
            .execute()
            .actionGet();
        long numResults = response.getHits().getTotalHits();  //same as .totalHits() ?
        return numResults;
    }
    

    /**
     * Example
     */
    public static void main(String[] args) {
        Client client = ESClient.MakeTransportClient("localhost", 9300);
        DeleteIndex.delete(client, "indextest");
        CreateIndex.create(client, "indextest");
        InsertDocument.insert(client, "indextest", "indextype1", "{\"f1\":\"dog\"}", "1");
        InsertDocument.insert(client, "indextest", "indextype2", "{\"f1\":\"cat\"}", "2");
        InsertDocument.insert(client, "indextest", "indextype2", "{\"f1\":\"horse\"}", "3");
        long numDocs1 = CountDocuments.count(client, "indextest");  //expect 3
        long numDocs2 = CountDocuments.count(client, "indextest", "indextype1");  //expect 1
        long numDocs3 = CountDocuments.count(client, "indextest", "indextype2");  //expect 2
        
        System.out.println("indextest counts: " + numDocs1 + ", " + numDocs2 + ", " + numDocs3);
    }
}
