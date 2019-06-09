package net.ruixin.solr;

import net.ruixin.solr.exception.SolrClientException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Vector;

/**
 * @author gai
 */
public class ClientManager implements IClientManager{
    private String solrUrl;
    /**
     * 是否是集群
     */
    private String isCloud;
    /**
     * 连接池大小
     */
    private int poolSize=1;
    public static final String HTTP="http://";

    private static List<SolrClient> searchClientPool = new Vector<>();

    public ClientManager(String solrUrl, String isCloud, int poolSize) {
        this.solrUrl = solrUrl;
        this.isCloud = isCloud;
        this.poolSize = poolSize;
        initSolrClientPool();
    }
    /**
     * 初始化SolrClient连接池
     */
    private void initSolrClientPool() {
        // 初始化检索用SolrClient连接池
        for (int i = 0; i < poolSize; i++) {
            searchClientPool.add(makeCloudSolrClient());
        }
    }

    private synchronized SolrClient makeCloudSolrClient() {
        SolrClient client=null;
        if("1".equals(isCloud)){
            final List<String> hosts = new ArrayList<String>();
            final String[] hosts0=solrUrl.split(",");
            for (int i = 0; i < hosts0.length; i++) {
                hosts.add(HTTP+hosts0[i]);
            }
            CloudSolrClient cloudSolrClient =
                    new CloudSolrClient.Builder(hosts)
                            .withConnectionTimeout(10000)
                            .withSocketTimeout(60000)
                            .build();
            //设置collection缓存的存活时间，单位 分钟
            cloudSolrClient.setCollectionCacheTTl(2);
            //索引优化
            //client.optimize();
            cloudSolrClient.connect();
            client=cloudSolrClient;
        } else {
            client = new HttpSolrClient.Builder(HTTP+solrUrl)
                    .withConnectionTimeout(10000)
                    .withSocketTimeout(60000)
                    .build();
        }
        return client;
    }

    @Override
    public SolrClient getSearchClient() {
        return searchClientPool.get((new Random()).nextInt(poolSize));
    }

    @Override
    public List<ConcurrentUpdateSolrClient> getUpdateClient(String collection) {
        throw new SolrClientException("通用工具类未实现该方法");
    }

    @Override
    public void close() throws Exception {
        if(searchClientPool==null){
            return;
        }
        for (int i = 0; i < searchClientPool.size(); i++) {
            if(searchClientPool.get(i)!=null){
                searchClientPool.get(i).close();
            }
        }
    }
}
