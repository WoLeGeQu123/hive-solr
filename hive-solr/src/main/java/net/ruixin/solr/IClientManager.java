package net.ruixin.solr;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;

import java.util.List;

/**
 * @author gai
 */
public interface IClientManager extends AutoCloseable {
    /**
     * 获取检索用SolrClient
     *
     * @return
     */
    public SolrClient getSearchClient();
    /**
     * 获取维护索引用SolrClient
     * collection
     * @return
     */
    public List<ConcurrentUpdateSolrClient> getUpdateClient(String collection);
}
