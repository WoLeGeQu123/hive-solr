package net.ruixin.solr.cloud;

import net.ruixin.solr.IClientManager;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;

import java.util.*;

/**
 * <p>
 * SolrClient工具类
 * </p>
 * @author gai
 */
public class CloudClientManager implements IClientManager {

    /**
     * 连接池大小
     */
    private int poolSize=1;
    private String solrUrl;
    public static final String HTTP="http://";
    /**
     * CloudSolrClient 连接池
     */
    private static List<CloudSolrClient> searchClientPool = new Vector<>();

    public CloudClientManager(String solrUrl, int poolSize) {
        this.poolSize=poolSize;
        this.solrUrl=solrUrl;
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

    private synchronized CloudSolrClient makeCloudSolrClient() {
        final List<String> hosts = new ArrayList<String>();
        final String[] hosts0=solrUrl.split(",");
        for (int i = 0; i < hosts0.length; i++) {
            hosts.add(HTTP+hosts0[i]);
        }
        CloudSolrClient client =
                new CloudSolrClient.Builder(hosts)
                        .withConnectionTimeout(10000)
                        .withSocketTimeout(60000)
                        .build();
        //设置collection缓存的存活时间，单位 分钟
        client.setCollectionCacheTTl(2);
        //索引优化
        //client.optimize();
        client.connect();
        return client;
    }

    /**
     * 获取检索用SolrClient
     *
     * @return
     */
    @Override
    public CloudSolrClient getSearchClient() {
        return searchClientPool.get((new Random()).nextInt(poolSize));
    }

    private List<String> getBaseSolrUrl() {
        CloudSolrClient client = getSearchClient();
        Set<String> liveNodes = client.getClusterStateProvider().getLiveNodes();
        List<String> baseUrls=new ArrayList<>(liveNodes.size());
        if (liveNodes == null) {
            return baseUrls;
        }
        for(String baseSolrUrl:liveNodes){
            if (baseSolrUrl != null) {
                baseSolrUrl = baseSolrUrl.replaceAll("_", "/");
                baseUrls.add(baseSolrUrl);
            }
        }
        return baseUrls;
    }

    /**
     * 获取维护索引用SolrClient
     *
     * @return
     */
    @Override
    public List<ConcurrentUpdateSolrClient> getUpdateClient(String collection) {
        int cussThreadCount = 2;
        int cussQueueSize = 10;
        List<String> baseUrls = getBaseSolrUrl();
        List<ConcurrentUpdateSolrClient> list=new ArrayList<>(baseUrls.size());
        for (String baseSolrUrl:baseUrls){
            ConcurrentUpdateSolrClient client
                    = (new ConcurrentUpdateSolrClient.Builder("http://" + baseSolrUrl + "/" + collection))
                    .withQueueSize(cussQueueSize)
                    .withThreadCount(cussThreadCount).build();
            list.add(client);
        }
        return list;
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