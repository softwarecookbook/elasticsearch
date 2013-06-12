/* See the file "NOTICE" for the full license governing this code. */

package com.swcb.es;

import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.indices.IndexMissingException;

public class RefreshIndex {

    /**
     * Refresh one or more indices.  Uses totalShards > 0 to determine if the
     * refresh operation succeeded.
     * Throws IndexMissingException if an index doesn't already exist.
     * @param indexNames a single index name or an array of index names
     * @return true if index was refreshed, false otherwise
     */
    @SuppressWarnings("unused")
    public static boolean refresh(Client client, String ... indexNames) {

        try {
            // Will throw an IndexMissingException if an index is not present
            RefreshResponse refreshResponse = client.admin().indices().
                    prepareRefresh(indexNames).execute().actionGet();
            int numShards = refreshResponse.getTotalShards();
            int numFailedShards = refreshResponse.getFailedShards();
            int numSuccessfulShards = refreshResponse.getSuccessfulShards();
            
            return numShards > 0;  //true if refresh found > 0 shards for this index
        } catch(IndexMissingException e) {
            e.printStackTrace();
        } catch(Exception e) {
            e.printStackTrace();
        } 
        return false;
    }

    /**
     * Test program
     */
    public static void main(String[] args) {
        Client client = ESClient.MakeTransportClient("localhost", 9300);
        
        boolean res = RefreshIndex.refresh(client, "twittertest");
        System.out.println("Index twittertest refreshed? " + res);
        res = RefreshIndex.refresh(client, new String[] {"twittertest1", "twitter"});
        System.out.println("Indices twitter, twittertest refreshed? " + res);
    }
}
