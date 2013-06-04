package com.swcb.es;

import static org.junit.Assert.*;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class BooleanQueryTest {

    static Client client;
    
    static final String INDEX_NAME = "indextest";
    static final String INDEX_TYPE1 = "indextype1";
    
    static final String DOC1_TEXT = "my dog has fleas";
    static final String DOC1 = "{\"f1\":\"" + DOC1_TEXT + "\"}";    
    static final String DOCNUM1 = "1";

    
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
    }

    @After
    public void tearDown() throws Exception {
        DeleteIndex.delete(client, INDEX_NAME);
    }
        
    @Test
    public final void testBooleanQueryClientStringStringStringObject() {
        InsertDocument.insert(client, INDEX_NAME, INDEX_TYPE1, DOC1, DOCNUM1);
        RefreshIndex.refresh(client, INDEX_NAME);

        // Create arrays of 0 or more must, should and must-not query terms
        // This query will match since must and not fields are matches
        QueryBuilder mqbMust, mqbShould, mqbNot;
        mqbMust   = MatchQuery.query("f1", "dog");
        mqbShould = MatchQuery.query("f1", "horse");
        mqbNot    = MatchQuery.query("f1", "cat");        
        BoolQueryBuilder bqb = 
            BooleanQuery.query(new QueryBuilder[] {mqbMust}, 
                  new QueryBuilder[] {mqbShould}, 
                  new QueryBuilder[] {mqbNot});
        SearchResponse response = Search.search(client, bqb, new String[] {INDEX_NAME}, new String[] {INDEX_TYPE1});
        SearchHits hits = response.getHits();
        assertTrue(hits.getTotalHits() == 1);
        
        // Create arrays of 0 or more must, should and must-not query terms
        // This query will not match since the must field does not match
        mqbMust   = MatchQuery.query("f1", "horse");
        mqbShould = MatchQuery.query("f1", "dog");
        mqbNot    = MatchQuery.query("f1", "cat");
        bqb = BooleanQuery.query(new QueryBuilder[] {mqbMust}, 
                    new QueryBuilder[] {mqbShould}, 
                    new QueryBuilder[] {mqbNot});
        response = Search.search(client, bqb, new String[] {INDEX_NAME}, new String[] {INDEX_TYPE1});
        hits = response.getHits();
        assertTrue(hits.getTotalHits() == 0);
        
        // This query will match since the must-not field holds
        bqb = BooleanQuery.query(null, null, new QueryBuilder[] {mqbNot});
        response = Search.search(client, bqb, new String[] {INDEX_NAME}, new String[] {INDEX_TYPE1});
        hits = response.getHits();
        assertTrue(hits.getTotalHits() == 1);
        
        // Use combinations of different query types (e.g. match and phrase)
        mqbMust   = PhraseQuery.query("f1", "my dog");
        mqbShould = MatchQuery.query("f1", "horse");
        mqbNot    = MatchQuery.query("f1", "cat");
        bqb = BooleanQuery.query(new QueryBuilder[] {mqbMust}, 
                    new QueryBuilder[] {mqbShould}, 
                    new QueryBuilder[] {mqbNot});
        response = Search.search(client, bqb, new String[] {INDEX_NAME}, new String[] {INDEX_TYPE1});
        hits = response.getHits();
        assertTrue(hits.getTotalHits() == 1);
    }
}
