package com.swcb.es;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import org.junit.Test;
import static org.junit.Assert.*;

public class CreateIndexTest {

    String indexName = "twittertest";

    @Test
    public final void createIndexTest() {
	Client client = new TransportClient()
	.addTransportAddress(new InetSocketTransportAddress("localhost", 9300));

	CreateIndexRequestBuilder createIndexRequestBuilder = 
	    client.admin().indices().prepareCreate(indexName);
	try {
	    CreateIndexResponse cir = createIndexRequestBuilder.execute().actionGet();
	    assertTrue( cir.isAcknowledged());
	} catch (org.elasticsearch.indices.IndexAlreadyExistsException e) {
	    fail(e.toString());			
	} catch (org.elasticsearch.indices.InvalidIndexNameException e) {
	    fail(e.toString());			
	} catch(Exception e) {
	    fail(e.toString());
	}
    }
}
