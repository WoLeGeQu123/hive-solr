package net.ruixin.solr.cloud;

import net.ruixin.solr.exception.SolrZkException;
import org.apache.log4j.Logger;
import org.apache.solr.common.cloud.SolrZkClient;
import org.apache.solr.common.cloud.ZkConfigManager;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.apache.solr.common.cloud.ZkConfigManager.CONFIGS_ZKNODE;

/**
 * <p>
 * CollectionConfigManager工具类
 * </p>
 *
 * @author gai
 */
public class CollectionConfigManager {
    /**
     * 本地配置文件位置
     */
    private String localConfigPath = "";
    private String defaultConfig = "_default";
    private String defaultSuffix = "_config";

    private Logger log = Logger.getLogger(CollectionConfigManager.class);

    private ZkClientManager clientManager;

    public CollectionConfigManager(ZkClientManager clientManager) {
        this.clientManager = clientManager;
    }

    public CollectionConfigManager(String zkHost) {
        this.clientManager = new ZkClientManager(zkHost);
    }

    /**
     * 下载默认配置
     */
    public void downDefaultConfig() {
        downConfig(defaultConfig, "");
        log.info("***下载默认配置***");
    }

    /**
     * 上传默认配置
     */
    public void upDefaultConfig() {
        upConfig(defaultConfig, "");
        log.info("***上传默认配置***");
    }

    /**
     * 下载zk下默认的collectionConfig到本地
     *
     * @param collection 配置名
     * @param suffix     配置名后缀
     */
    public void downConfig(String collection, String suffix) {
        SolrZkClient solrZkClient = clientManager.getZkClient();
        Path path = Paths.get(localConfigPath + getConfigName(collection,suffix));
        try {
            solrZkClient.downConfig(defaultConfig, path);
        } catch (IOException e) {
            throw new SolrZkException("下载配置文件错误", e);
        }
        log.info("***下载zk的collectionConfig到本地【" + collection + "】***");
    }
    /**
     * 下载zk下默认的collectionConfig到本地
     *
     * @param collection 配置名
     */
    public void downConfig(String collection) {
        downConfig(collection,null);
    }

    /**
     * 上传collectionConfig到zk
     *
     * @param collection 配置名
     * @param suffix     配置名后缀
     */
    public void upConfig(String collection, String suffix) {
        SolrZkClient solrZkClient = clientManager.getZkClient();
        String collectionName = getConfigName(collection,suffix);
        Path path = Paths.get(localConfigPath + collectionName);
        try {
            solrZkClient.upConfig(path, collectionName);
        } catch (IOException e) {
            throw new SolrZkException("上传配置文件错误", e);
        }
        log.info("***上传collectionConfig到zk【" + collection + "】***");
    }

    /**
     * 上传collectionConfig到zk
     *
     * @param collection 配置名
     */
    public void upConfig(String collection) {
        upConfig(collection,null);
    }
    /**
     * 复制默认配置生成新的collection配置
     *
     * @param collection 配置名称
     * @param suffix     配置名后缀
     */
    public void copyDefaultConfig(String collection, String suffix) {
        ZkConfigManager zkConfigManager = clientManager.getZkConfigManager();
        String collectionName = getConfigName(collection,suffix);
        //solrZkClient.upConfig(path, collectionName);
        try {
            zkConfigManager.copyConfigDir(defaultConfig, collectionName);
        } catch (IOException e) {
            throw new SolrZkException("复制生成新collection错误", e);
        }
        log.info("***复制默认配置生成新的collection配置【" + collection + "】***");
    }

    /**
     * 复制默认配置生成新的collection配置
     *
     * @param collection 配置名称
     */
    public void copyDefaultConfig(String collection) {
        copyDefaultConfig(collection,null);
    }

    /**
     * 检测默认配置是否存在
     *
     * @return
     */
    public boolean existsDefaultConfig() {
        log.info("***检测默认配置是否存在***");
        return exists(defaultConfig,null);
    }

    /**
     * 检测配置是否存在
     *
     * @param collection 配置名称
     * @param suffix     配置名后缀
     * @return
     */
    public boolean exists(String collection, String suffix) {
        SolrZkClient solrZkClient = clientManager.getZkClient();
        log.info("***检测配置是否存在【" + collection + "】***");
        try {
            return solrZkClient.exists(CONFIGS_ZKNODE + "/" + getConfigName(collection,suffix),
                    false);
        } catch (KeeperException | InterruptedException e) {
            throw new SolrZkException("检测配置是否存在错误", e);
        }
    }


    /**
     * 检测配置是否存在
     *
     * @param collection 配置名称
     * @return
     */
    public boolean exists(String collection) {
        return exists(collection,null);
    }

    /**
     * 删除collection配置
     * @param collection 配置名称
     * @param suffix     配置名后缀
     */
    public void deleteConfig(String collection, String suffix) {
        ZkConfigManager zkConfigManager = clientManager.getZkConfigManager();
        log.info("***删除collection配置【" + collection + "】***");
        try {
            zkConfigManager.deleteConfigDir(getConfigName(collection,suffix));
        } catch (IOException e) {
            throw new SolrZkException("删除collection配置", e);
        }
    }

    /**
     * 删除collection配置
     * @param collection 配置名称
     */
    public void deleteConfig(String collection) {
        deleteConfig(collection,null);
    }

    public String getConfigName(String collection, String suffix){
        return collection + (suffix == null ? defaultSuffix : suffix);
    }

    public void setDefaultConfig(String defaultConfig) {
        this.defaultConfig = defaultConfig;
    }

    public void setLocalConfigPath(String localConfigPath) {
        this.localConfigPath = localConfigPath;
    }

    public void setDefaultSuffix(String defaultSuffix) {
        if(defaultSuffix!=null&&!defaultSuffix.isEmpty()){
            this.defaultSuffix = defaultSuffix;
        }
    }
}