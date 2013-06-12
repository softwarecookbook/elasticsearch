/* See the file "NOTICE" for the full license governing this code. */

package com.swcb.es;

import org.elasticsearch.client.Client;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;

public class BooleanQuery {

    static final String INDEX_NAME = "indextest";
    static final String INDEX_TYPE1 = "indextype1";
    
    // JSON document to be inserted
    static final String DOC1_TEXT = "my dog has fleas";
    static final String DOC1 = "{\"f1\":\"" + DOC1_TEXT + "\"}";    
    static final String DOCNUM1 = "1";


    /**
     * Create a Boolean query using zero or more must, should and must-not 
     * sub-queries.  The sub-queries can be any type of query.
     * 
     * @param mustQueries zero or more must queries
     * @param shouldQueries zero or more should queries
     * @param mustNotQueries zero or more must-not queries
     * @return BoolQueryBuilder object
     */
    public static BoolQueryBuilder query(
            QueryBuilder[] mustQueries, QueryBuilder[] shouldQueries, QueryBuilder[] mustNotQueries) {
        BoolQueryBuilder bqb = QueryBuilders.boolQuery();
        if (mustQueries != null) {
            for (int i = 0; i < mustQueries.length; i++) {
                bqb.must(mustQueries[i]);
            }
        }
        if (shouldQueries != null) {
            for (int i = 0; i < shouldQueries.length; i++) {
                bqb.should(shouldQueries[i]);
            }
        }
        if (mustNotQueries != null) {
            for (int i = 0; i < mustNotQueries.length; i++) {
                bqb.mustNot(mustNotQueries[i]);
            }
        }
        return bqb;
    }
    
      
    /**
     * Example boolean query using Lucene syntax.  
     *   1. A document is parsed into separate words using the default analyzer 
     *      and added to a new index. 
     *   2. The boolean query is composed of zero or more must, should and 
     *      must-not queries.
     */
    public static void main(String[] args) {
        Client client = ESClient.MakeTransportClient("localhost", 9300);

        // Index a document into a new index
        DeleteIndex.delete(client, INDEX_NAME);
        CreateIndex.create(client, INDEX_NAME);
        InsertDocument.insert(client, INDEX_NAME, INDEX_TYPE1, DOC1, DOCNUM1);
        RefreshIndex.refresh(client, INDEX_NAME);

        // Create arrays of 0 or more must, should and must-not query terms
        // This query will match since must and not fields are matches
        QueryBuilder mqbMust, mqbShould, mqbNot;
        mqbMust   = MatchQuery.query("f1", "dog");
        mqbShould = MatchQuery.query("f1", "horse");
        mqbNot    = MatchQuery.query("f1", "cat");        
        BoolQueryBuilder bqb = 
            query(new QueryBuilder[] {mqbMust}, 
                  new QueryBuilder[] {mqbShould}, 
                  new QueryBuilder[] {mqbNot});
        SearchResponse response = Search.search(client, bqb, new String[] {INDEX_NAME}, new String[] {INDEX_TYPE1});
        SearchHits hits = response.getHits();
        System.out.println("Expected one hit, found: " + (hits.getTotalHits()) + " hits");
        
        // Create arrays of 0 or more must, should and must-not query terms
        // This query will not match since the must field does not match
        mqbMust   = MatchQuery.query("f1", "horse");
        mqbShould = MatchQuery.query("f1", "dog");
        mqbNot    = MatchQuery.query("f1", "cat");
        bqb = query(new QueryBuilder[] {mqbMust}, 
                    new QueryBuilder[] {mqbShould}, 
                    new QueryBuilder[] {mqbNot});
        response = Search.search(client, bqb, new String[] {INDEX_NAME}, new String[] {INDEX_TYPE1});
        hits = response.getHits();
        System.out.println("Expected zero hits, found: " + (hits.getTotalHits()) + " hits");
        
        // This query will match since the must-not field holds
        bqb = query(null, null, new QueryBuilder[] {mqbNot});
        response = Search.search(client, bqb, new String[] {INDEX_NAME}, new String[] {INDEX_TYPE1});
        hits = response.getHits();
        System.out.println("Expected one hit, found: " + (hits.getTotalHits()) + " hits");
        
        // Use combinations of different query types (e.g. match and phrase)
        mqbMust   = PhraseQuery.query("f1", "my dog");
        mqbShould = MatchQuery.query("f1", "horse");
        mqbNot    = MatchQuery.query("f1", "cat");
        bqb = query(new QueryBuilder[] {mqbMust}, 
                    new QueryBuilder[] {mqbShould}, 
                    new QueryBuilder[] {mqbNot});
        response = Search.search(client, bqb, new String[] {INDEX_NAME}, new String[] {INDEX_TYPE1});
        hits = response.getHits();
        System.out.println("Expected one hit, found: " + (hits.getTotalHits()) + " hits");        
    }
}
