package net.ruixin.solr.cloud;

import net.ruixin.solr.exception.SolrZkException;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;

/**
 * @author gai
 */
public class ZkClientManager implements AutoCloseable {
    private String zkHost;
    private static ZkClientManager instance;
    private SolrZkClient zkClient;
    private ZkConfigManager zkConfigManager;
    public ZkClientManager(String zkHost) {
        this.zkHost = zkHost;
        zkClient=new SolrZkClient(zkHost,30000);
        zkConfigManager=new ZkConfigManager(zkClient);
        instance=this;
    }
    public static synchronized ZkClientManager getInstance() {
        if (instance == null) {
            throw new SolrZkException("ZkClientManager初始化错误");
        }
        return instance;
    }

    public SolrZkClient getZkClient() {
        return zkClient;
    }

    public ZkConfigManager getZkConfigManager() {
        return zkConfigManager;
    }

    @Override
    public void close() throws Exception {
        if(zkClient!=null){
            zkClient.close();
        }
    }
}
