package net.ruixin.hive.solr.format;

import java.io.IOException;

import net.ruixin.hive.solr.ConfigurationUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;

public class SolrTableCursor {
	private SolrClient server;
	private int start;
	private String collection;
	private int count;
	private int numBufferRows;
	private String[] fields;

	private SolrDocumentList buffer;
	private int pos = 0;
	private long numFound;

	SolrTableCursor(String url,boolean isCould, String collection,String[] fields, int start, int count, int numBufferRows) throws IOException {
        server = ConfigurationUtil.getSolrClient(url,isCould);
        this.fields = fields;
        this.start = start;
        this.count = count;
        this.collection = collection;

        this.numBufferRows = numBufferRows;
        fetchNextDocumentChunk();
	}

	private void fetchNextDocumentChunk() throws IOException {
        SolrQuery query = new SolrQuery();
        query.setStart(start);
        query.setRows(numBufferRows);
        query.setFields(fields);
        query.set("qt", "/select");
        query.setQuery("*:*");
		QueryResponse response;
		try {
			response = server.query(collection,query);
		} catch (SolrServerException e) {
			throw new IOException(e);
		}
		buffer = response.getResults();
		numFound = buffer.getNumFound();
		start += buffer.size();
		count -= buffer.size();
		pos = 0;
	}
	
	SolrDocument nextDocument() throws IOException {
		if (pos >= buffer.size()) {
			fetchNextDocumentChunk();
			
			if (pos >= buffer.size()) {
				return null;
			}
		}
		
		return buffer.get(pos++);
	}
	
	long getNumFound() {
		return numFound;
	}
}
