package net.ruixin.hive.solr.format;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import net.ruixin.hive.solr.ConfigurationUtil;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.common.SolrInputDocument;

/**
 * @author gai
 */
public class SolrTable {
	private static final int MAX_INPUT_BUFFER_ROWS = 100000;
	private static final int MAX_OUTPUT_BUFFER_ROWS = 100000;
	private SolrClient server;
	private String url;
	private String collection;
	private boolean isCloud;
	private Collection<SolrInputDocument> outputBuffer;
	private int numInputBufferRows = MAX_INPUT_BUFFER_ROWS;
	private int numOutputBufferRows = MAX_OUTPUT_BUFFER_ROWS;	

	public SolrTable(String url,boolean isCloud,String collection) {
        this.server = ConfigurationUtil.getSolrClient(url,isCloud);
        this.url = url;
        this.collection = collection;
        this.isCloud = isCloud;
        this.outputBuffer = new ArrayList<SolrInputDocument>(numOutputBufferRows);
	}

	public void save(SolrInputDocument doc) throws IOException {
		outputBuffer.add(doc);

		if (outputBuffer.size() >= numOutputBufferRows) {
			flush();
		}
	}
	
	public void flush() throws IOException {
		try {
			if (!outputBuffer.isEmpty()) {
				server.add(collection,outputBuffer);
				outputBuffer.clear();
			}
		} catch (SolrServerException e) {
			throw new IOException(e);
		}
	}

	public long count() throws IOException {
		return findAll(new String[0], 0, 0).getNumFound();
	}

	public SolrTableCursor findAll(String[] fields, int start, int count) throws IOException {
		return new SolrTableCursor(url, isCloud,collection, fields, start, count, numInputBufferRows);
	}
	
	public void drop() throws IOException{
		try {
			server.deleteByQuery(collection,"*:*");
			server.commit(collection);
		} catch (SolrServerException e) {
			throw new IOException(e);
		}
	}

	public void commit() throws IOException {
		try {
			flush();
			server.commit(collection);
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}

	public void rollback() throws IOException {
		try {
			outputBuffer.clear();
			server.rollback(collection);
		} catch (SolrServerException e) {
			throw new IOException(e.getMessage(), e);
		}
	}

	public int getNumOutputBufferRows() {
		return numOutputBufferRows;
	}

	public void setNumOutputBufferRows(int numOutputBufferRows) {
		this.numOutputBufferRows = numOutputBufferRows;
	}

	public int getNumInputBufferRows() {
		return numInputBufferRows;
	}

	public void setNumInputBufferRows(int numInputBufferRows) {
		this.numOutputBufferRows = numInputBufferRows;
	}
	
}