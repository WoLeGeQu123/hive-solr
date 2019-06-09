package net.ruixin.hive.solr;

import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;

import com.google.common.collect.ImmutableSet;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

/**
 * @author gai
 */
public class ConfigurationUtil {
	public static final String HTTP="http://";
	public static final String URL = "solr.url";
	public static final String COLUMN_MAPPING = "solr.column.mapping";
	public static final String BUFFER_IN_ROWS = "solr.buffer.input.rows";
	public static final String BUFFER_OUT_ROWS = "solr.buffer.output.rows";
	/**
	 * solrcloud模式下，设置集合名
	 */
	public static final String COLLECTION_NAME = "solr.collection.name";
	/**
	 * 表所在位置
	 */
	public static final String TABLE_LOCATION = "location";
	/**
	 * 是否solrcloud
	 */
	public static final String IS_CLOUD = "solr.is.cloud";
	public static final Set<String> ALL_PROPERTIES = ImmutableSet.of(URL, COLUMN_MAPPING,
														BUFFER_IN_ROWS, BUFFER_OUT_ROWS,
														COLLECTION_NAME,IS_CLOUD,TABLE_LOCATION);

	public final static String getColumnMapping(Configuration conf) {
		return conf.get(COLUMN_MAPPING);
	}

	public final static String getUrl(Configuration conf) {
		return conf.get(URL);
	}
	
	public final static int getNumInputBufferRows(Configuration conf) {
		String value = conf.get(BUFFER_IN_ROWS, "10000");
		return Integer.parseInt(value);
	}
	
	public final static int getNumOutputBufferRows(Configuration conf) {
		String value = conf.get(BUFFER_OUT_ROWS, "10000");
		return Integer.parseInt(value);
	}
	
	public static void copySolrProperties(Properties from, JobConf to) {
    	for (String key : ALL_PROPERTIES) {
            String value = from.getProperty(key);
            if (value != null) {
            	to.set(key, value);
            }
    	}
	}

    public static void copySolrProperties(Properties from,
            Map<String, String> to) {
    	for (String key : ALL_PROPERTIES) {
            String value = from.getProperty(key);
            if (value != null) {
                to.put(key, value);
            }
    	}
    }
	
	public static String[] getAllColumns(String columnMappingString) {
		String[] columns = columnMappingString.split(",");
		
		String[] trimmedColumns = new String[columns.length];
		for (int i = 0; i < columns.length; i++) {
			trimmedColumns[i] = columns[i].trim();
		}
		return trimmedColumns;
	}

	public static List<String> getSolrUrls(String url){
		String[] servers=url.split(",");
		List<String> solrUrls=new ArrayList<>(servers.length);
		for (int i = 0; i < servers.length; i++) {
			solrUrls.add(HTTP+servers[i]);
		}
		return solrUrls;
	}

	public static boolean isCloud(Configuration conf){
		return "1".equals(conf.get(IS_CLOUD));
	}

	/**
	 * 获取SolrClient
	 * @param url
	 * @param isCloud
	 * @return
	 */
	public static SolrClient getSolrClient(String url,boolean isCloud){
		if(isCloud){
			return new CloudSolrClient.Builder(getSolrUrls(url))
					.withConnectionTimeout(300*1000)
					.withSocketTimeout(300*1000)
					.build();
		} else {
			return new HttpSolrClient.Builder(HTTP+url)
					.withConnectionTimeout(300*1000)
					.withSocketTimeout(300*1000)
					.build();
		}
	}
	/**
	 * 获取collectionName
	 * @param conf
	 * @return
	 */
	public static String getCollectionName(JobConf conf){
		String temp=conf.get(COLLECTION_NAME);
		if(temp==null||temp.isEmpty()){
			String t=conf.get(TABLE_LOCATION);
			return t.substring(t.lastIndexOf("/")+1);
		}
		return temp;
	}
}