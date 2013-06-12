/* See the file "NOTICE" for the full license governing this code. */

package com.swcb.es;

import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingResponse;
import org.elasticsearch.action.deletebyquery.DeleteByQueryResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.indices.TypeMissingException;

/**
 * Delete the index using a variety of techniques:
 *   http://stackoverflow.com/questions/8019221/how-to-erase-elasticsearch-index
 * 1. Clear the index by removing all documents, but don't delete
 * 2. Delete the index and all documents  
 *
 */
public class DeleteIndex {

    /**
     * Remove all docs in index, but don't delete the index itself
     * Note: the index should be refreshed before using - no API available to do this.
     * @param indexName
     * @return DeleteByQueryResponse object
     */
    public static DeleteByQueryResponse clearIndex(Client client, String indexName) {
        try {
            DeleteByQueryResponse response = client.prepareDeleteByQuery(indexName)
                .setQuery(QueryBuilders.matchAllQuery())
                .execute().actionGet();
            return response;
        } catch (IndexMissingException e) {
        }
        return null;
    }
    
    /**
     * Remove all docs in index for this type, but don't delete the index itself
     * Note: the index should be refreshed before using
     * Note: the API does not throw a TypeMissingException.  If the type does not
     * exist no errors are observed.
     * @param indexName
     * @param indexType
     * @return DeleteByQueryResponse object
     */
    public static DeleteByQueryResponse clearIndex(Client client, String indexName, String indexType) {
        try {
            DeleteByQueryResponse response = client.prepareDeleteByQuery(indexName)
                .setQuery(QueryBuilders.matchAllQuery()).setTypes(indexType)
                .execute().actionGet();
            return response;
        } catch (IndexMissingException e) {
        }
        return null;
    }
    
    /**
     * Delete indexName
     * @return deleteIndexResponse isAcknowledged=true if index successfully deleted
     */
    public static DeleteIndexResponse delete(Client client, String indexName) {
        try {
            DeleteIndexResponse deleteIndexResponse = client.admin().indices()
                .delete(new DeleteIndexRequest(indexName)).actionGet();
            return deleteIndexResponse;
        } catch (IndexMissingException e) {
        }
        return null;
    }

    
    /**
     * Delete the indexType documents in indexName.  Does this also delete the type?
     * http://elasticsearch-users.115913.n3.nabble.com/Java-API-delete-all-documents-within-the-specific-index-and-the-specific-type-td3094518.html
     * @return deleteIndexResponse isAcknowledged=true if index successfully deleted
     */
    public static DeleteMappingResponse delete(Client client, String indexName, String indexType) {
        try {
            DeleteMappingResponse response = client.admin().indices()
                    .prepareDeleteMapping(indexName)
                    .setType(indexType)
                    .execute().actionGet();
            return response;
        } catch (TypeMissingException e) {
        } catch (IndexMissingException e) {
        }
        return null;
    }

    
    /**
     * Example
     */
    public static void main(String[] args) {
        Client client = ESClient.MakeTransportClient("localhost", 9300);
        DeleteIndexResponse response = DeleteIndex.delete(client, "indextest");
        boolean result = response != null ? response.isAcknowledged() : false;
        System.out.println("indextest delete " + (result ? "succeeded": "failed"));
    }
}
