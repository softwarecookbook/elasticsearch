package com.swcb.es;

import static org.junit.Assert.*;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class MatchAllQueryTest {

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
        InsertDocument.insert(client, INDEX_NAME, INDEX_TYPE1, DOC1, DOCNUM1);
        RefreshIndex.refresh(client, INDEX_NAME);
    }

    @After
    public void tearDown() throws Exception {
        DeleteIndex.delete(client, INDEX_NAME);
    }
        
    @Test
    public final void testMatchQueryClientStringStringStringObject() {
        // This match query will match since the document is parsed into words and indexed
        MatchAllQueryBuilder mqb = MatchAllQuery.query();
        SearchResponse response = Search.search(client, mqb, new String[] {INDEX_NAME}, new String[] {INDEX_TYPE1});
        SearchHits hits = response.getHits();
        assertTrue(hits.getTotalHits() == 1);
    }
}
