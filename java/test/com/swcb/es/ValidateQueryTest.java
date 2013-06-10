package com.swcb.es;

import static org.junit.Assert.*;

import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class ValidateQueryTest {

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
    public final void testValidate() {
        // Validate an incorrect query-string-query (double colons f1::fleas)
        QueryStringQueryBuilder qsqb = QueryStringQuery.query("f1::fleas AND f2:happy");
        ValidateQueryResponse response = ValidateQuery.validate(client, qsqb, "indextest");
        assertNotNull(response);
        assertFalse(response.isValid());

        // Validate a correct query-string-query
        qsqb = QueryStringQuery.query("f1:fleas AND f2:happy");
        response = ValidateQuery.validate(client, qsqb, "indextest");
        assertNotNull(response);
        assertTrue(response.isValid());
    }
}
