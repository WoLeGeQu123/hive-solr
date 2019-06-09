package net.ruixin.solr.cloud;

import net.ruixin.solr.SolrCollection;
import net.ruixin.solr.SolrCopyField;
import net.ruixin.solr.SolrField;
import net.ruixin.solr.exception.SolrClientException;
import org.apache.log4j.Logger;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest.Delete;
import org.apache.solr.client.solrj.request.schema.SchemaRequest;
import org.apache.solr.client.solrj.response.CollectionAdminResponse;
import org.apache.solr.client.solrj.response.schema.SchemaResponse;
import org.apache.solr.common.StringUtils;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.util.NamedList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;


/**
 * <p>
 * Solr Collection 工具类
 * </p>
 * @author gai
 */
public class CloudCollectionManager {

    private Logger log = Logger.getLogger(CloudCollectionManager.class);
    private CloudClientManager clientManager;
    private CollectionConfigManager configManager;
    public CloudCollectionManager(CloudClientManager clientManager, CollectionConfigManager configManager){
        this.clientManager=clientManager;
        this.configManager=configManager;
    }

    /**
     * 创建Collection
     */
    public void createCollection(SolrCollection collection, List<SolrField> columns) {
        // 创建collection
        createCollection(collection);
        // 定义Field
        createFields(collection.getName(), columns);
        log.info("CollectionManager.createCollection");
    }

    public void createCollection(SolrCollection collection) {
        if (isExist(collection.getName())) {
            return;
        } else {
            setZKConfig(collection.getConfig(),collection.getConfigSuffix());
            // 创建Collection
            // collection不存在
            CollectionAdminRequest.Create create =
                    CollectionAdminRequest
                            .Create
                            .createCollection(collection.getName(),
                                    configManager.getConfigName(collection.getName(), collection.getConfigSuffix()),
                                    collection.getNumShards(),
                                    collection.getNumReplicas());
            // 配置Collection
            create.setMaxShardsPerNode(collection.getMaxShardsPerNode());
            CollectionAdminResponse response = null;
            try {
                response = create.process(clientManager.getSearchClient());
            } catch (SolrServerException | IOException e) {
                throw new SolrClientException("创建Collection失败",e);
            }

            log.info(response);

            if(!response.isSuccess()){
                throw new SolrClientException("创建Collection失败",response.getException());
            }

        }
        log.info("CollectionManager.createCollection");
    }


    /**
     * 根据数据表字段创建Field
     */
    public void createFields(String collection, List<SolrField> fields) {
        log.info("CollectionManager.createFields");
        List<SolrField> schemaFields = getSchemaFields(collection);
        List<SchemaRequest.Update> list = new ArrayList<>();
        List<SolrField> updateFields=new ArrayList<>();
        for (SolrField field : fields) {
            // schema中没有该字段
            if (schemaFields != null) {
                if(!schemaFields.contains(field.getName())){
                    // 增加到字段到Collection
                    SchemaRequest.AddField addField = new SchemaRequest.AddField(field.toHashMap());
                    list.add(addField);
                } else {
                    updateFields.add(field);
                }
            } else {
                throw new SolrClientException("createFields失败");
            }
        }
        updateFields(collection,updateFields);
        SchemaRequest.MultiUpdate multiUpdateRequest = new SchemaRequest.MultiUpdate(list);
        try {
            SchemaResponse.UpdateResponse multipleUpdatesResponse = multiUpdateRequest.process(clientManager.getSearchClient(), collection);
            log.info(multipleUpdatesResponse.getRequestUrl() + " : " + multipleUpdatesResponse.getResponseHeader());
        } catch (SolrServerException | IOException e) {
            throw new SolrClientException("createFields失败",e);
        }
    }

    /**
     * 使用copyFields创建union域
     * 添加CopyField 复制域    主要参数source、dest,复制与只要将多个域组合成一个域
     */
    public void createCopyFields(String collection, List<SolrCopyField> copyFields) {
        log.info("CollectionManager.createCopyFields");
        List<SolrCopyField> schemaCopyFields = getSchemaCopyFields(collection);
        List<SolrCopyField> createFields = new ArrayList<>();
        for (SolrCopyField copyField : copyFields) {
            if (!schemaCopyFields.contains(copyField)) {
                createFields.add(copyField);
            }
        }

        List<SchemaRequest.Update> list = new ArrayList<>();
        for (final SolrCopyField field : createFields) {
            if(!StringUtils.equals(field.getDest(),field.getSource())){
                SchemaRequest.AddCopyField addCopyField = new SchemaRequest.AddCopyField(field.getSource(),
                        new ArrayList<String>(){{this.add(field.getDest());}});
                list.add(addCopyField);
            }
        }
        SchemaRequest.MultiUpdate multiUpdateRequest = new SchemaRequest.MultiUpdate(list);
        try {
            SchemaResponse.UpdateResponse response = multiUpdateRequest.process(clientManager.getSearchClient(), collection);
            NamedList<Object> result = response.getResponse();
            log.info(result.toString());
        } catch (SolrServerException | IOException e) {
            throw new SolrClientException("createCopyFields失败",e);
        }
    }

    /**
     * 删除CopyFields
     */
    public void deleteCopyFields(String connection, List<SolrCopyField> copyFields) {
        List<SchemaRequest.Update> list = new ArrayList<>();
        for (final SolrCopyField field:copyFields) {
            SchemaRequest.DeleteCopyField deleteField = new SchemaRequest.DeleteCopyField(field.getSource(),
                    new ArrayList<String>(){{this.add(field.getDest());}});
            list.add(deleteField);
        }
        SchemaRequest.MultiUpdate multiUpdateRequest = new SchemaRequest.MultiUpdate(list);
        try {
            SchemaResponse.UpdateResponse response = multiUpdateRequest.process(clientManager.getSearchClient(), connection);
            log.info(response.getRequestUrl());
        } catch (Exception e) {
            throw new SolrClientException("deleteCopyField删除失败",e);
        }
    }
    /**
     * 删除CopyField
     */
    public void deleteCopyField(String connection, final SolrCopyField copyField) {
        deleteCopyFields(connection,new ArrayList<SolrCopyField>(){{this.add(copyField);}});
    }

    /**
     * 查看所有copyFields的union域
     */
    public List<SolrCopyField> getSchemaCopyFields(String collection) {
//              String fieldname="name";
        List<SolrCopyField> copyFields = new ArrayList<>();
        ModifiableSolrParams params = new ModifiableSolrParams();
        params.add("wt", "json");
        // 设置返回sourcefield信息
//        params.add("source.fl", "name");
        //params.add("dest.fl","union");
        SchemaRequest.CopyFields allCopyFields = new SchemaRequest.CopyFields(params);
        try {
            SchemaResponse.CopyFieldsResponse response = allCopyFields.process(clientManager.getSearchClient(), collection);
            NamedList<Object> result = response.getResponse();
            List<NamedList<Object>> lists = (List<NamedList<Object>>) result.get("copyFields");
            for (NamedList<Object> copyField : lists) {
                SolrCopyField solrCopyField=new SolrCopyField();
                for (Entry<String, Object> entry : copyField) {
                    String key = entry.getKey();
                    Object val = entry.getValue();
                    if (key.equals("source")) {
                        solrCopyField.setSource(val.toString());
                    }
                    if (key.equals("dest")) {
                        solrCopyField.setDest(val.toString());
                    }
                }
                copyFields.add(solrCopyField);
            }

        } catch (SolrServerException | IOException e) {
            throw new SolrClientException("getSchemaCopyFields失败",e);
        }
        return copyFields;
    }

    /**
     * 删除Fields
     */
    public void deleteFields(String collection, List<SolrField> fields) {
        List<SolrCopyField> schemaCopyFields = getSchemaCopyFields(collection);
        List<SolrCopyField> deleteCopyFields =new ArrayList<>();
        for (int i = 0; i < schemaCopyFields.size(); i++) {
            for (int j = 0; j < fields.size(); j++) {
                if(fields.get(j).getName().equals(schemaCopyFields.get(i).getDest())||
                        fields.get(j).getName().equals(schemaCopyFields.get(i).getSource())){
                    deleteCopyFields.add(schemaCopyFields.get(i));
                }
            }
        }
        deleteCopyFields(collection,deleteCopyFields);

        List<SchemaRequest.Update> list = new ArrayList<>();
        for (SolrField field:fields) {
            SchemaRequest.DeleteField deleteField = new SchemaRequest.DeleteField(field.getName());
            list.add(deleteField);
        }
        SchemaRequest.MultiUpdate multiUpdateRequest = new SchemaRequest.MultiUpdate(list);
        try {
            SchemaResponse.UpdateResponse response = multiUpdateRequest.process(clientManager.getSearchClient(), collection);
            log.info(response.getRequestUrl());
        } catch (Exception e) {
            throw new SolrClientException("deleteFields异常!",e);
        }
    }

    /**
     * 删除Field
     */
    public void deleteField(String collection, final SolrField field) {
        deleteFields(collection,new ArrayList<SolrField>(){{this.add(field);}});
    }


    /**
     * 更新Field
     */
    public void updateFields(String collection, List<SolrField> fields) {
        List<SchemaRequest.Update> list = new ArrayList<>();
        for (SolrField field:fields) {
            SchemaRequest.ReplaceField replaceField = new SchemaRequest.ReplaceField(field.toHashMap());
            list.add(replaceField);
        }
        SchemaRequest.MultiUpdate multiUpdateRequest = new SchemaRequest.MultiUpdate(list);
        try {
            SchemaResponse.UpdateResponse response = multiUpdateRequest.process(clientManager.getSearchClient(), collection);
            log.info(response.getRequestUrl());
        } catch (Exception e) {
            throw new SolrClientException("updateField异常!",e);
        }
    }


    /**
     * 检查Collection是否存在
     */
    private boolean isExist(String collection) {
        boolean isConnection = false;
        try {
            isConnection = clientManager.getSearchClient().getClusterStateProvider().getClusterState().hasCollection(collection);
        } catch (IOException e) {
            throw new SolrClientException("检查Collection是否存在失败",e);
        }
        if (isConnection) {
            log.info(collection + "CollectionManager.isExist");
        } else {
            log.info(collection + "CollectionManager.totExist");
        }
        return isConnection;
    }

    /**
     * 配置Collection
     */
    private void setZKConfig(String collection, String suffix) {
        //先创建并且上传collectionConfig
        if (!configManager.exists(collection,suffix)) {
            configManager.copyDefaultConfig(collection,suffix);
        }
        log.info("CollectionManager.setZKConfig");
    }

    /**
     * 返回collection所有Field的定义信息
     *
     * @param collection
     * @return
     */
    public List<SolrField> getSchemaFields(String collection) {

        List<SolrField> AllField = new ArrayList<>();
        ModifiableSolrParams params = new ModifiableSolrParams();
        //是否显示域类型的默认信息
        params.add("showDefaults", "false");
        //是否返回动态域的定义信息
//        params.add("includeDynamic", "true");
        //指定返回那些域的定义信息
//        params.add("fl", "name,_version_");
        //指定不返回哪些类型
        SchemaRequest.Fields allFields = new SchemaRequest.Fields(params);
        SchemaResponse.FieldsResponse response;
        try {
            response = allFields.process(clientManager.getSearchClient(), collection);
            NamedList<Object> result = response.getResponse();
            @SuppressWarnings("unchecked")
            List<NamedList<Object>> fields = (List<NamedList<Object>>) result.get("fields");
            for (NamedList<Object> field : fields) {
                SolrField solrField=new SolrField();
                for (Entry<String, Object> entry : field) {
                    String key = entry.getKey();
                    Object val = entry.getValue();
                    if (key.equals("name")) {
                        if (val == null) {
                            solrField.setName("");
                        } else {
                            solrField.setName(val.toString());
                        }
                    }
                    if (key.equals("type")) {
                        if (val == null) {
                            solrField.setType("");
                        } else {
                            solrField.setType(val.toString());
                        }
                    }
                    if (key.equals("indexed")) {
                        if (val != null) {
                            solrField.setIndexed(Boolean.valueOf(val.toString()));
                        }
                    }
                    if (key.equals("stored")) {
                        if (val != null) {
                            solrField.setStored(Boolean.valueOf(val.toString()));
                        }
                    }
                    if (key.equals("multiValued")) {
                        if (val != null) {
                            solrField.setMultiValued(Boolean.valueOf(val.toString()));
                        }
                    }
                }
                AllField.add(solrField);
            }

        } catch (SolrServerException | IOException e) {
            throw new SolrClientException("返回collection所有Field的定义信息异常!",e);
        }

        return AllField;
    }

    /**
     * 删除collection
     *
     * @param collection
     */
    public void deleteCollection(SolrCollection collection) {
        if (this.isExist(collection.getName())) {
            log.info("***collection:" + collection);
            Delete delete = CollectionAdminRequest.deleteCollection(collection.getName());
            try {
                delete.process(clientManager.getSearchClient());
            } catch (SolrServerException | IOException e) {
                throw new SolrClientException("删除collection异常!",e);
            }
        } else {
            log.info("***找不到要删除的collection[" + collection + "]***");
        }
        configManager.deleteConfig(collection.getConfig(),collection.getConfigSuffix());
    }
}
