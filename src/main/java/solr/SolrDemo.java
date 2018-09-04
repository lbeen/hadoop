package solr;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;

import java.io.IOException;

/**
 * @author 李斌
 */
public class SolrDemo {
    @Test
    public void insert() throws IOException, SolrServerException {
        SolrInputDocument document = new SolrInputDocument();
        document.addField("id", "4");
        document.addField("text_smart", "红米手机");
        HttpSolrClient client = getClient();
        client.add(document);
        client.commit();
        client.close();
    }

    @Test
    public void update() throws IOException, SolrServerException {
        SolrInputDocument document = new SolrInputDocument();
        document.addField("id", "3");
        document.addField("text_smart", "红米手机");
        HttpSolrClient client = getClient();
        client.add(document);
        client.commit();
        client.close();
    }

    @Test
    public void delete() throws IOException, SolrServerException {
        HttpSolrClient client = getClient();
        client.deleteById("4");
        client.commit();
        client.close();
    }

    @Test
    public void query() throws IOException, SolrServerException {
        HttpSolrClient client = getClient();
        SolrQuery params = new SolrQuery();
        params.setQuery("text_smart:红米手机");
        QueryResponse response = client.query(params);
        SolrDocumentList list = response.getResults();
        for (SolrDocument document : list) {
            System.out.println(document.get("id") + "---" + document.get("text_smart"));
        }
        client.close();
    }

    private HttpSolrClient getClient() {
        HttpSolrClient.Builder builder = new HttpSolrClient.Builder();
        builder.withBaseSolrUrl("http://192.168.1.111:8080/solr/collection1");
        return builder.build();
    }
}
