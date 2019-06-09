package net.ruixin.solr;

import net.ruixin.solr.exception.SolrQueryException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.params.MapSolrParams;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author gai
 */
public class SolrQueryManager {

    private IClientManager clientManager;

    public SolrQueryManager(IClientManager clientManager) {
        this.clientManager = clientManager;
    }

    /**
     * 查询数据
     * @param queryParamMap
     * @param indexName
     * @return
     */
    public SolrDocumentList query(Map<String, String> queryParamMap, String indexName){
        MapSolrParams queryParams = new MapSolrParams(queryParamMap);
        QueryResponse response = null;
        try {
            response = this.clientManager.getSearchClient().query(indexName, queryParams);
        } catch (SolrServerException | IOException e) {
            throw new SolrQueryException("查询失败",e);
        }
        return response.getResults();
    }

    /**
     * 查询数据
     * @param solrQuery
     * @param indexName
     * @return
     */
    public SolrDocumentList query(SolrQuery solrQuery, String indexName){

        QueryResponse response = null;
        try {
            response = this.clientManager.getSearchClient().query(indexName, solrQuery);
        } catch (SolrServerException e) {
            throw new SolrQueryException("solr服务器出现异常",e);
        } catch (IOException e) {
            throw new SolrQueryException("查询失败",e);
        }
        return response.getResults();
    }

    /**
     * 添加文档
     */
    public void addDoc(SolrInputDocument doc, String indexName){
        SolrClient client=clientManager.getSearchClient();
        try {
            client.add(indexName, doc);
            // Indexed documents must be committed
            client.commit(indexName);
        } catch (SolrServerException | IOException e) {
            try {
                client.rollback(indexName);
            } catch (SolrServerException e1) {
            } catch (IOException e1) {
            }
            throw new SolrQueryException("插入失败",e);
        }
    }

    /**
     * 添加文档
     */
    public void addDocs(List<SolrInputDocument> docs, String indexName){

        SolrClient client=clientManager.getSearchClient();
        try {
            client.add(indexName, docs);
            // Indexed documents must be committed
            client.commit(indexName);
        } catch (SolrServerException | IOException e) {
            try {
                client.rollback(indexName);
            } catch (SolrServerException e1) {
            } catch (IOException e1) {
            }
            throw new SolrQueryException("插入失败",e);
        }
    }

    /**
     * 添加文档
     */
    public void addBean(Object obj, String indexName){
        SolrClient client=clientManager.getSearchClient();
        try {
            client.addBean(indexName, obj);
            // Indexed documents must be committed
            client.commit(indexName);
        } catch (SolrServerException | IOException e) {
            try {
                client.rollback(indexName);
            } catch (SolrServerException e1) {
            } catch (IOException e1) {
            }
            throw new SolrQueryException("插入失败",e);
        }
    }

    /**
     * 添加文档
     */
    public void addBeans(List objs, String indexName){
        SolrClient client=clientManager.getSearchClient();
        try {
            client.addBeans(indexName, objs);
            // Indexed documents must be committed
            client.commit(indexName);
        } catch (SolrServerException | IOException e) {
            try {
                client.rollback(indexName);
            } catch (SolrServerException e1) {
            } catch (IOException e1) {
            }
            throw new SolrQueryException("插入失败",e);
        }
    }

    /**
     * 清空集合
     * @param collection
     */
    public void emptyCollection(String collection){
        SolrClient client=clientManager.getSearchClient();
        try {
            client.deleteByQuery(collection,"*:*");
            // Indexed documents must be committed
            client.commit(collection);
        } catch (SolrServerException | IOException e) {
            try {
                client.rollback(collection);
            } catch (SolrServerException e1) {
            } catch (IOException e1) {
            }
            throw new SolrQueryException("删除collection失败",e);
        }
    }
}
