/* See the file "NOTICE" for the full license governing this code. */

package com.swcb.es;

import org.elasticsearch.client.Client;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHits;

/**
 * Query string queries use a query parser and support field names with AND/OR 
 * Boolean operators, phrases and parentheses.  For example:
 *   f1:"has fleas" AND (f1:dog OR f2:cat)");
 */
public class QueryStringQuery {

    static final String INDEX_NAME = "indextest";
    static final String INDEX_TYPE1 = "indextype1";

    // Simple test documents with two fields
    static final String DOC1 = "{\"f1\":\"my dog has fleas\",\"f2\":\"my cat has fleas\"}";    
    static final String DOCNUM1 = "1";
    static final String DOC2 = "{\"f1\":\"my dog is happy\",\"f2\":\"my cat is happy\"}";    
    static final String DOCNUM2 = "2";


    /**
     * Create a query string query using the supplied query string.  ElasticSearch
     * supports AND/OR Boolean operators and parentheses for operator ordering.
     * 
     * @param queryString
     * @return QueryStringQueryBuilder object
     */
    public static QueryStringQueryBuilder query(String queryString) {     
        QueryStringQueryBuilder mqb = QueryBuilders.queryString(queryString) ;
        return mqb;
    }
    
      
    /**
     * Example query string queries.
     */
    public static void main(String[] args) {
        Client client = ESClient.MakeTransportClient("localhost", 9300);

        // Index a document into a new index
        DeleteIndex.delete(client, INDEX_NAME);
        CreateIndex.create(client, INDEX_NAME);
        InsertDocument.insert(client, INDEX_NAME, INDEX_TYPE1, DOC1, DOCNUM1);
        InsertDocument.insert(client, INDEX_NAME, INDEX_TYPE1, DOC2, DOCNUM2);
        RefreshIndex.refresh(client, INDEX_NAME);

        // This query will not match any document due to the AND operator
        QueryStringQueryBuilder qb = query("f1:fleas AND f2:happy");
        SearchResponse response = Search.search(client, qb, new String[] {INDEX_NAME}, new String[] {INDEX_TYPE1});
        SearchHits hits = response.getHits();
        System.out.println("Expected zero hits, found: " + (hits.getTotalHits()) + " hits");

        // This query matches the first document using a phrase for the first term
        qb = query("f1:\"has fleas\" AND (f1:dog OR f2:cat)");
        response = Search.search(client, qb, new String[] {INDEX_NAME}, new String[] {INDEX_TYPE1});
        hits = response.getHits();
        System.out.println("Expected one hit, found: " + (hits.getTotalHits()) + " hits");
        
        // This query will match the first document using multiple operators
        qb = query("f1:fleas AND (f2:cat OR f2:happy)");
        response = Search.search(client, qb, new String[] {INDEX_NAME}, new String[] {INDEX_TYPE1});
        hits = response.getHits();
        System.out.println("Expected one hit, found: " + (hits.getTotalHits()) + " hits");

        // This query is malformed and would throw an exception if we didn't check
        qb = query("f1:fleas AND f2:cat OR f2:happy)");  //missing left parentheses
        ValidateQueryResponse validateQueryResponse = ValidateQuery.validate(client, qb, INDEX_NAME);
        if ((validateQueryResponse != null) && (validateQueryResponse.isValid())) {
            System.out.println("Expected an invalid query, but found valid query");
        }
        else {
            System.out.println("Expected an invalid query, found invalid query");
        }

        // This query is malformed and will throw an exception
        // It is better to validate the query as above before throwing an exception
        qb = query("f1:fleas AND f2:cat OR f2:happy)");  //missing left parentheses
        try {
            response = Search.search(client, qb, new String[] {INDEX_NAME}, new String[] {INDEX_TYPE1});
            hits = response.getHits();
            System.out.println("Expected one hit, found: " + (hits.getTotalHits()) + " hits");
        } catch (SearchPhaseExecutionException e) {
            System.out.println("malformed query aborted");
        }
        
        // Clean-up
        DeleteIndex.delete(client, INDEX_NAME);
    }
}
