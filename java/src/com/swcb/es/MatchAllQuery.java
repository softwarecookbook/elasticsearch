package com.swcb.es;

import org.elasticsearch.client.Client;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;

public class MatchAllQuery {

    static final String INDEX_NAME = "indextest";
    static final String INDEX_TYPE1 = "indextype1";
    
    static final String DOC1_TEXT = "my dog has fleas";
    static final String DOC1 = "{\"f1\":\"" + DOC1_TEXT + "\"}";    
    static final String DOCNUM1 = "1";


    /**
     * Query matches all documents in index
     * 
     * @return MatchQueryBuilder object or null if error
     */
    public static MatchAllQueryBuilder query() {     
        MatchAllQueryBuilder mqb = QueryBuilders.matchAllQuery();
        return mqb;
    }
    
      
    /**
     * Example match query.  A new index is created and the match_all query
     * matches all documents in the index.
     */
    public static void main(String[] args) {
        Client client = ESClient.MakeTransportClient("localhost", 9300);
        
        // Index a document into a new index
        DeleteIndex.delete(client, INDEX_NAME);
        CreateIndex.create(client, INDEX_NAME);
        InsertDocument.insert(client, INDEX_NAME, INDEX_TYPE1, DOC1, DOCNUM1);
        RefreshIndex.refresh(client, INDEX_NAME);

        // This query matches all documents in the index
        MatchAllQueryBuilder mqb = query();
        SearchResponse response = Search.search(client, mqb, new String[] {INDEX_NAME}, new String[] {INDEX_TYPE1});
        SearchHits hits = response.getHits();
        System.out.println("Expected one hit, found: " + (hits.getTotalHits()) + " hits");
    }
}
