/* See the file "NOTICE" for the full license governing this code. */

package com.swcb.es;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.InvalidIndexNameException;

public class CreateIndex {

    /**
     * Create indexName
     * @param indexName
     * @return CreateIndexResponse with isAcknowledged=true if index successfully created
     */
    public static CreateIndexResponse create(Client client, String indexName) {
        CreateIndexRequestBuilder createIndexRequestBuilder = 
            client.admin().indices().prepareCreate(indexName);
        try {
            CreateIndexResponse response = createIndexRequestBuilder.execute().actionGet();
            return response;
        } catch (IndexAlreadyExistsException e) {
            e.printStackTrace();
        } catch (InvalidIndexNameException e) {
            e.printStackTrace();
        } catch(Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Example create index
     */
    public static void main(String[] args) {
        Client client = ESClient.MakeTransportClient("localhost", 9300);
        CreateIndexResponse response = CreateIndex.create(client, "indextest");
        boolean result = response.isAcknowledged();
        System.out.println("indextest create " + (result ? "succeeded": "failed"));
    }
}
