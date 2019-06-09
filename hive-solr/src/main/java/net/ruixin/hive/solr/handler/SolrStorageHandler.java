package net.ruixin.hive.solr.handler;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import net.ruixin.hive.solr.ConfigurationUtil;
import net.ruixin.hive.solr.serde2.SolrSerDe;
import net.ruixin.hive.solr.format.SolrInputFormat;
import net.ruixin.hive.solr.format.SolrOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.security.authorization.DefaultHiveAuthorizationProvider;
import org.apache.hadoop.hive.ql.security.authorization.HiveAuthorizationProvider;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;

public class SolrStorageHandler extends DefaultStorageHandler {
    private Configuration mConf = null;

    public SolrStorageHandler() {
    }

    @Override
    public void configureTableJobProperties(TableDesc tableDesc, Map<String, String> jobProperties) {
        Properties properties = tableDesc.getProperties();
        ConfigurationUtil.copySolrProperties(properties, jobProperties);
    }

    @Override
    public Class<? extends InputFormat> getInputFormatClass() {
        return SolrInputFormat.class;
    }

    @Override
    public HiveMetaHook getMetaHook() {
        return new DummyMetaHook();
    }

    @Override
    public Class<? extends OutputFormat> getOutputFormatClass() {
        return SolrOutputFormat.class;
    }

    @Override
    public Class<? extends SerDe> getSerDeClass() {
        return SolrSerDe.class;
    }

    @Override
    public Configuration getConf() {
        return this.mConf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.mConf = conf;
    }

    private class DummyMetaHook implements HiveMetaHook {

        @Override
        public void commitCreateTable(Table tbl) throws MetaException {
            // nothing to do...
        }

        @Override
        public void commitDropTable(Table tbl, boolean deleteData)
                throws MetaException {
            boolean isExternal = MetaStoreUtils.isExternalTable(tbl);
            if (deleteData && isExternal) {
                // nothing to do...
            } else if (deleteData && !isExternal) {
                String url = tbl.getParameters().get(ConfigurationUtil.URL);
                String collectionName = tbl.getParameters().get(ConfigurationUtil.COLLECTION_NAME);
                if (collectionName == null || collectionName.isEmpty()) {
                    String t = tbl.getParameters().get(ConfigurationUtil.TABLE_LOCATION);
                    collectionName = t.substring(t.lastIndexOf("/") + 1);
                }
                boolean isCould = "1".equals(tbl.getParameters().get(ConfigurationUtil.IS_CLOUD));
                SolrClient server = ConfigurationUtil.getSolrClient(url, isCould);
                try {
                    server.deleteByQuery(collectionName, "*:*");
                    server.commit();
                } catch (SolrServerException e) {
                    throw new MetaException(e.getMessage());
                } catch (IOException e) {
                    throw new MetaException(e.getMessage());
                }
            }
        }

        @Override
        public void preCreateTable(Table tbl) throws MetaException {
            // nothing to do...
        }

        @Override
        public void preDropTable(Table tbl) throws MetaException {
            // nothing to do...
        }

        @Override
        public void rollbackCreateTable(Table tbl) throws MetaException {
            // nothing to do...
        }

        @Override
        public void rollbackDropTable(Table tbl) throws MetaException {
            // nothing to do...
        }

    }

    @Override
    public void configureInputJobProperties(TableDesc tableDescription, Map<String, String> jobProperties) {
        Properties properties = tableDescription.getProperties();
        ConfigurationUtil.copySolrProperties(properties, jobProperties);
    }

    @Override
    public void configureJobConf(TableDesc tableDescription, JobConf config) {
        Properties properties = tableDescription.getProperties();
        ConfigurationUtil.copySolrProperties(properties, config);
    }

    @Override
    public void configureOutputJobProperties(TableDesc tableDescription, Map<String, String> jobProperties) {
        Properties properties = tableDescription.getProperties();
        ConfigurationUtil.copySolrProperties(properties, jobProperties);
    }

    @Override
    public HiveAuthorizationProvider getAuthorizationProvider()
            throws HiveException {
        return new DefaultHiveAuthorizationProvider();
    }
}