package com.swcb.es;

import java.util.List;

import org.elasticsearch.action.admin.indices.validate.query.QueryExplanation;
import org.elasticsearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.indices.IndexMissingException;

/**
 * ValidateQuery is used to validate a potentially expensive query without
 * executing it.
 * 
 * @see http://www.elasticsearch.org/guide/reference/api/validate/
 */
public class ValidateQuery {

    static final String INDEX_NAME = "indextest";
    static final String INDEX_TYPE1 = "indextype1";

    // Simple test documents with two fields
    static final String DOC1 = "{\"f1\":\"my dog has fleas\",\"f2\":\"my cat has fleas\"}";    
    static final String DOCNUM1 = "1";
    static final String DOC2 = "{\"f1\":\"my dog is happy\",\"f2\":\"my cat is happy\"}";    
    static final String DOCNUM2 = "2";

    /**
     * Validate a query against one or more indices
     * 
     * Throws IndexMissingException if an index doesn't already exist.
     * @param indexNames a single index name or an array of index names
     * @return true if query is valid, false otherwise
     */
    public static ValidateQueryResponse validate(
            Client client, QueryBuilder queryBuilder, String ... indexNames) {
        try {
            ValidateQueryResponse validateResponse = client.admin().indices()
                .prepareValidateQuery(indexNames)
                .setQuery(queryBuilder)
                .setExplain(true)       //optional, default false
                .execute().actionGet();
            return validateResponse;
        } catch(IndexMissingException e) { //thrown if index not present
            e.printStackTrace();
        }
        return null;
    }

    /**
     * Print out the explanation(s) for the validated query
     * @param response
     */
    static void printExplanations(ValidateQueryResponse response) {
        boolean isValid = response.isValid();
        if (isValid) {
            List<? extends QueryExplanation> explanations = response.getQueryExplanation();
            for (int i = 0; i < explanations.size(); i++) {
                QueryExplanation exp = explanations.get(i);
                isValid = exp.isValid();
                String explanation = exp.getExplanation();
                String index = exp.getIndex();
                System.out.println("Query: " + explanation + " on index: " 
                        + index + " is valid? " + isValid);
            }
        }
        else {
            List<? extends QueryExplanation> explanations = response.getQueryExplanation();
            for (int i = 0; i < explanations.size(); i++) {
                QueryExplanation exp = explanations.get(i);
                isValid = exp.isValid();
                String error = exp.getError();
                String index = exp.getIndex();
                System.out.println("Error: " + error + " on index: " 
                        + index + " is valid? " + isValid);
            }
        }
    }
    
    /**
     * Validate query examples
     */
    public static void main(String[] args) {
        Client client = ESClient.MakeTransportClient("localhost", 9300);
        
        // Index a document into a new index
        DeleteIndex.delete(client, INDEX_NAME);
        CreateIndex.create(client, INDEX_NAME);
        InsertDocument.insert(client, INDEX_NAME, INDEX_TYPE1, DOC1, DOCNUM1);
        InsertDocument.insert(client, INDEX_NAME, INDEX_TYPE1, DOC2, DOCNUM2);
        RefreshIndex.refresh(client, INDEX_NAME);

        // Validate an incorrect query-string-query (double colons f1::fleas)
        QueryStringQueryBuilder qsqb = QueryStringQuery.query("f1::fleas AND f2:happy");
        ValidateQueryResponse response = validate(client, qsqb, "indextest");
        if (response != null) {
            printExplanations(response);
        }

        // Validate a correct query-string-query
        qsqb = QueryStringQuery.query("f1:fleas AND f2:happy");
        response = validate(client, qsqb, "indextest");
        if (response != null) {
            printExplanations(response);
        }

        // Clean-up
        DeleteIndex.delete(client, INDEX_NAME);
    }
}
