package com.swcb.es;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.InvalidIndexNameException;

public class CreateIndex {

    /**
     * Create indexName
     * @return true if index successfully created
     */
    public boolean createIndex(String indexName) {
        boolean result = false;

        Client client = new TransportClient()
        .addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

        CreateIndexRequestBuilder createIndexRequestBuilder = 
            client.admin().indices().prepareCreate(indexName);
        try {
            CreateIndexResponse cir = createIndexRequestBuilder.execute().actionGet();
            result = cir.isAcknowledged();
        } catch (IndexAlreadyExistsException e) {
            e.printStackTrace();
        } catch (InvalidIndexNameException e) {
            e.printStackTrace();
        } catch(Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void main(String[] args) {
        CreateIndex ci = new CreateIndex();
        boolean result = ci.createIndex("twittertest");
        System.out.println("twittertest create " + (result ? "succeeded": "failed"));
    }
}
