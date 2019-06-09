package net.ruixin.solr;

import java.io.Serializable;

/**
 * @author gai
 */
public class SolrCollection implements Serializable {
    /**
     * the collection name
     */
    private String name;
    /**
     * the collection config
     */
    private String config;
    /**
     * 配置后缀
     */
    private String configSuffix;
    /**
     * the number of shards in the collection
     * 分片数
     */
    private Integer numShards;
    /**
     * the replication factor of the collection (same as numNrtReplicas)
     * 副本数
     */
    private Integer numReplicas;
    /**
     * 每个节点最大分片数
     */
    private Integer maxShardsPerNode=new Integer(3);

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getConfig() {
        return config;
    }

    public void setConfig(String config) {
        this.config = config;
    }

    public Integer getNumShards() {
        return numShards;
    }

    public void setNumShards(Integer numShards) {
        this.numShards = numShards;
    }

    public Integer getNumReplicas() {
        return numReplicas;
    }

    public void setNumReplicas(Integer numReplicas) {
        this.numReplicas = numReplicas;
    }

    public String getConfigSuffix() {
        return configSuffix;
    }

    public void setConfigSuffix(String configSuffix) {
        this.configSuffix = configSuffix;
    }

    public Integer getMaxShardsPerNode() {
        return maxShardsPerNode==null?3:maxShardsPerNode;
    }

    public void setMaxShardsPerNode(Integer maxShardsPerNode) {
        this.maxShardsPerNode = maxShardsPerNode;
    }
}
