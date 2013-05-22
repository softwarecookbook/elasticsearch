package com.swcb.es;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.client.Client;

/**
 * Add the schema mapping for an existing index and type
 */
public class PutMapping {

    /**
     * Add this mapping for the existing index and type
     * @param mapping
     * @param indexName
     * @param indexType
     * @return PutMappingResponse object containing isAcknowledge field
     */
    public static PutMappingResponse put(Client client, String mapping, String indexName, String indexType) {
        
            // Add the mapping to the index
            PutMappingResponse response = client.admin().indices()
                .preparePutMapping(indexName)
                .setType(indexType)
                .setSource(mapping)
                .execute().actionGet();
            return response;
    }

    /**
     * Demonstrate put mapping
     */
    public static void main(String[] args) {
        String indexName = "indextest";
        String indexType = "indextype";

        // Start with a fresh index
        Client client = ESClient.MakeTransportClient("localhost", 9300);
        DeleteIndex.delete(client, indexName);
        CreateIndex.create(client, indexName);

        // Test mapping: { "indextype" : { "properties" : { "message" : "date" } } }
        String mapping = "{\"" + indexType + "\":{\"properties\":{\"message\":\"date\"}}}";
        PutMappingResponse response = PutMapping.put(client, mapping, indexName, indexType);
        boolean res = response != null ? response.isAcknowledged() : null;
        System.out.println("Put Mapping returned: " + res);
    }
}
