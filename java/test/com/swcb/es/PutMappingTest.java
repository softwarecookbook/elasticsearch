/* See the file "NOTICE" for the full license governing this code. */

package com.swcb.es;

import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;

import org.junit.Test;
import static org.junit.Assert.*;

public class PutMappingTest {

    final String INDEX_NAME = "indextest";
    final String INDEX_TYPE = "indextype";
    
    /** Sample document using a text value for message field - this violates mapping */
    final String TEST_DOC_NO_DATE = "{\"message\":\"dog\"}";  //not a date
    
    /** Sample document with a date value for message field */
    final String TEST_DOC_WITH_DATE = "{\"message\":\"2013-04-13\"}";  //a date

    
    @Test
    public final void testPutMapping() {
        Client client = ESClient.MakeTransportClient("localhost", 9300);

        // Start with a fresh index
        DeleteIndex.delete(client, INDEX_NAME);
        CreateIndex.create(client, INDEX_NAME);

        // Test mapping: { "indextype" : { "properties" : { "message" : "date" } } }
        String mapping = "{\"" + INDEX_TYPE + "\":{\"properties\":{\"message\":{\"type\":\"date\"}}}}";
        PutMappingResponse putMappingResponse = PutMapping.put(client, mapping, INDEX_NAME, INDEX_TYPE);
        assertNotNull(putMappingResponse);
        assertTrue(putMappingResponse.isAcknowledged());

        // Now insert a document that violates the date mapping (no date)
        IndexResponse indexResponse = InsertDocument.insert(client, INDEX_NAME, INDEX_TYPE, TEST_DOC_NO_DATE, null);
        assertNull(indexResponse);
        
        // Now insert a doc that works with the mapping (has date)
        indexResponse = InsertDocument.insert(client, INDEX_NAME, INDEX_TYPE, TEST_DOC_WITH_DATE, null);
        assertNotNull(indexResponse);
        assertNotNull(indexResponse.getId());
        
        // Clean-up
        DeleteIndex.delete(client, INDEX_NAME);
    }
}
