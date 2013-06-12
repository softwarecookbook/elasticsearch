/* See the file "NOTICE" for the full license governing this code. */

package com.swcb.es;

import static org.junit.Assert.*;

import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class QueryStringQueryTest {

    static Client client;
    
    static final String INDEX_NAME = "indextest";
    static final String INDEX_TYPE1 = "indextype1";

    // Simple test documents with two fields
    static final String DOC1 = "{\"f1\":\"my dog has fleas\",\"f2\":\"my cat has fleas\"}";    
    static final String DOCNUM1 = "1";
    static final String DOC2 = "{\"f1\":\"my dog is happy\",\"f2\":\"my cat is happy\"}";    
    static final String DOCNUM2 = "2";

    
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        client = ESClient.MakeTransportClient("localhost", 9300);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        client = null;
    }

    @Before
    public void setUp() throws Exception {
        DeleteIndex.delete(client, INDEX_NAME);
        CreateIndex.create(client, INDEX_NAME);
        InsertDocument.insert(client, INDEX_NAME, INDEX_TYPE1, DOC1, DOCNUM1);
        InsertDocument.insert(client, INDEX_NAME, INDEX_TYPE1, DOC2, DOCNUM2);
        RefreshIndex.refresh(client, INDEX_NAME);
    }

    @After
    public void tearDown() throws Exception {
        DeleteIndex.delete(client, INDEX_NAME);
    }

    @Test
    public final void testQuery() {
        // This query will not match any document due to the AND operator
        QueryStringQueryBuilder qb = QueryStringQuery.query("f1:fleas AND f2:happy");
        SearchResponse response = Search.search(client, qb, new String[] {INDEX_NAME}, new String[] {INDEX_TYPE1});
        SearchHits hits = response.getHits();
        assertTrue(hits.getTotalHits() == 0);

        // This query matches the first document using a phrase for the first term
        qb = QueryStringQuery.query("f1:\"has fleas\" AND (f1:dog OR f2:cat)");
        response = Search.search(client, qb, new String[] {INDEX_NAME}, new String[] {INDEX_TYPE1});
        hits = response.getHits();
        assertTrue(hits.getTotalHits() == 1);
        
        // This query will match the first document using multiple operators
        qb = QueryStringQuery.query("f1:fleas AND (f2:cat OR f2:happy)");
        response = Search.search(client, qb, new String[] {INDEX_NAME}, new String[] {INDEX_TYPE1});
        hits = response.getHits();
        assertTrue(hits.getTotalHits() == 1);
        
        // This query is malformed and would throw an exception if not checked
        qb = QueryStringQuery.query("f1:fleas AND f2:cat OR f2:happy)");  //missing left parentheses
        ValidateQueryResponse validateQueryResponse = ValidateQuery.validate(client, qb, INDEX_NAME);
        assertNotNull(validateQueryResponse);
        assertFalse(validateQueryResponse.isValid());

        // This query is malformed and will throw an exception
        // It is better to validate the query above before throwing an exception
        qb = QueryStringQuery.query("f1:fleas AND f2:cat OR f2:happy)");  //missing left parentheses
        try {
            response = Search.search(client, qb, new String[] {INDEX_NAME}, new String[] {INDEX_TYPE1});
            hits = response.getHits();
            fail();
        } catch (SearchPhaseExecutionException e) {
            return;  //test passes
        }
    }

}
