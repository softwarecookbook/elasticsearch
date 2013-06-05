package com.swcb.es;

import org.elasticsearch.client.Client;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;

/**
 * A match query can be used as a boolean query, a phrase query and a prefix
 * phrase query.  
 * 
 * ElasticSearch supports two different Boolean queries:
 *   - The ElasticSearch match query is a traditional AND, OR query
 *   - The ElasticSearch bool query implements the Lucene Boolean query (must,
 *     should, must-not operators)
 *  
 *  This MatchQuery class implements the traditional (non-Lucene) Boolean query
 */
public class MatchQuery {

    static final String INDEX_NAME = "indextest";
    static final String INDEX_TYPE1 = "indextype1";
    
    static final String DOC1_TEXT = "my dog has fleas";
    static final String DOC1 = "{\"f1\":\"" + DOC1_TEXT + "\"}";    
    static final String DOCNUM1 = "1";
    
    /** Boolean values for match boolean operator */
    public enum BOOLEANS {AND, OR};


    /**
     * Create a match query using the optional field and required value
     * 
     * This match query defaults to a Boolean OR query
     * 
     * @param field optional - defaults to the default field
     * @param value required
     * @return MatchQueryBuilder object or null if error
     */
    public static MatchQueryBuilder query(String field, Object value) {     
        MatchQueryBuilder mqb = QueryBuilders.matchQuery(field, value);
        return mqb;
    }
    
    /**
     * Create a Boolean query using the field (optional), value and Boolean
     * operator.
     * 
     * @param field optional - defaults to the default field
     * @param value required
     * @param operator - OR, AND  A null value defaults to AND
     * @return
     */
    public static MatchQueryBuilder query(String field, Object value, BOOLEANS operator) {
        MatchQueryBuilder mqb = QueryBuilders.matchQuery(field, value);
        mqb.operator(MatchQueryBuilder.Operator.AND); //default
        if (operator == BOOLEANS.OR) {
            mqb.operator(MatchQueryBuilder.Operator.OR);
        }
        return mqb;
    }
      
    /**
     * Example match Boolean queries
     */
    public static void main(String[] args) {
        Client client = ESClient.MakeTransportClient("localhost", 9300);

        // Index a document into a new index
        DeleteIndex.delete(client, INDEX_NAME);
        CreateIndex.create(client, INDEX_NAME);
        InsertDocument.insert(client, INDEX_NAME, INDEX_TYPE1, DOC1, DOCNUM1);
        RefreshIndex.refresh(client, INDEX_NAME);

        // This query will match a single word in the document
        MatchQueryBuilder mqb = query("f1", "dog");
        SearchResponse response = 
            Search.search(client, mqb, new String[] {INDEX_NAME}, new String[] {INDEX_TYPE1});
        SearchHits hits = response.getHits();
        System.out.println("Expected one hit, found: " + (hits.getTotalHits()) + " hits");
        
        // This query will match using a default Boolean AND operator
        mqb = query("f1", DOC1_TEXT);
        response = 
            Search.search(client, mqb, new String[] {INDEX_NAME}, new String[] {INDEX_TYPE1});
        hits = response.getHits();
        System.out.println("Expected one hit, found: " + (hits.getTotalHits()) + " hits");

        // This query will match using a Boolean OR operator
        mqb = query("f1", "fleas ticks");
        response = 
            Search.search(client, mqb, new String[] {INDEX_NAME}, new String[] {INDEX_TYPE1});
        hits = response.getHits();
        System.out.println("Expected one hit, found: " + (hits.getTotalHits()) + " hits");

        // This query will fail using a Boolean AND operator
        mqb = query("f1", "fleas ticks", BOOLEANS.AND);
        response = 
            Search.search(client, mqb, new String[] {INDEX_NAME}, new String[] {INDEX_TYPE1});
        hits = response.getHits();
        System.out.println("Expected zero hits, found: " + (hits.getTotalHits()) + " hits");
        
        // Clean-up
        DeleteIndex.delete(client, INDEX_NAME);
    }
}
