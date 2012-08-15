package de.avgl.c3.solrWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Fieldable;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.SolrIndexReader;

import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.tdb.TDBFactory;


public class ResponseWriter implements QueryResponseWriter {
	
	private String tsPath = "";
	private Model model;
	
	private static final String FACETS_ID= "facets";
	private static final String HIGHLIGHTING_ID= "highlighting";
	private static final String HEADER_ID= "header";
	private static final String RESULTS_ID= "results";
	private static final String STORED_ID= "stored";
	private static final String SPARQL_ID= "sparql";
	private static final String NUMFOUND_ID= "numFound";
	
	private static Map<String, Object> finalJsonMap;
	static{
		finalJsonMap = new HashMap<String, Object>();
		finalJsonMap.put(FACETS_ID, new HashMap<String, Object>());
		finalJsonMap.put(HIGHLIGHTING_ID, new HashMap<String, Object>());
		finalJsonMap.put(HEADER_ID, new HashMap<String, String>());
		finalJsonMap.put(RESULTS_ID, new ArrayList<Object>());
	}
	
	
	
	@Override
	public void init(NamedList list) {
		tsPath = (String)list.get("storePath");
		model = TDBFactory.createDataset(tsPath).getDefaultModel();
	}

	/**
	 * Output
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void write(Writer writer, SolrQueryRequest request,
			SolrQueryResponse response) throws IOException {
		SolrParams params = request.getParams();
			
		NamedList lst = response.getValues();
		
	    Boolean omitHeader = request.getParams().getBool(CommonParams.OMIT_HEADER);
	    if(omitHeader != null && omitHeader){
	    	lst.remove("responseHeader");
	    }
	    int sz = lst.size();
	    int start=0;
	    
	    SolrIndexReader reader = request.getSearcher().getReader();
	    
	    for (int i=start; i<sz; i++) {
	    	
	    	if(lst.getVal(i) instanceof DocSlice){
	    		DocSlice slice = (DocSlice)lst.getVal(i);
	    		
    			//add num found to header
	    		((HashMap<String, String>)finalJsonMap.get(HEADER_ID)).put(NUMFOUND_ID, slice.size()+"");
	    		
	    		for (DocIterator it = slice.iterator(); it.hasNext(); ){
	    			Document doc = reader.document(it.nextDoc());
	    			
	    			//break if the field tsId is not existent / set
	    			String tsId=doc.get("tsId");
	    	    	if(tsId == null){
	    	    		writer.write("Field 'tsId' is NULL");
	    	    		return;
	    	    	}
	    	    	
	    	    	//Write all stored fields to json
	    	    	Map<String, Object> resMap= new HashMap<String, Object>();
	    	    	Iterator<Fieldable> fieldIt = doc.getFields().iterator();
	    	    	Map<String, String> storedMap= new HashMap<String, String>();
	    	    	resMap.put(STORED_ID, storedMap);
	    	    	
	    	    	while(fieldIt.hasNext()){
	    	    		Fieldable fieldable = fieldIt.next();
	    	    		if(fieldable instanceof Field){
	    					Field field = (Field)fieldable;
	    					if(field.isStored()){
	    						storedMap.put(field.name(), field.stringValue());
	    					}
	    				}
	    	    		
	    	    	}	    	    
	    	    
//	    	    	Query the Triplestore
	    	    	String query=getQuery(tsId, params.get("sparql"));
	    	    	System.out.println(query);
	    	    	QueryExecution qexec = QueryExecutionFactory.create(query, model) ;
	    	    	try{
	    	    		ResultSet results = qexec.execSelect();
	    	    		ByteArrayOutputStream out = new ByteArrayOutputStream();
	    			    
	    			    ResultSetFormatter.outputAsJSON(out, results);
	    			    resMap.put(SPARQL_ID,out.toString());
	    	    		  
	    	    	}finally{qexec.close();}
	    	    	((List<Object>)finalJsonMap.get(RESULTS_ID)).add(resMap);
	    		}
	    	}
	    	
	    }
	    
	    writer.write(getJson(finalJsonMap));
	}
	
	/**
	 * If "sparql"-param is not set, the standard will be chosen
	 * @param tsId
	 * @param sparqlTempQuery
	 * @return
	 */
	private String getQuery(String tsId, String sparqlTempQuery){
		String sparqlQuery = "Select ?tsPred ?tsObject where {?tsId ?tsPred ?tsObject}";
    	
    	String tsId2= "<"+tsId+">";
    	
    	if(sparqlTempQuery != null){
    		sparqlQuery = sparqlTempQuery.replaceAll("\\?tsId", tsId2);
    	}
    	else{
    		sparqlQuery = sparqlQuery.replaceAll("\\?tsId", tsId2);
    	}
    	return sparqlQuery;
	}
	

	@Override
	public String getContentType(SolrQueryRequest request,
			SolrQueryResponse response) {
		return CONTENT_TYPE_TEXT_UTF8;
	}
	
	public String getJson(Map<String, Object> map){
		CharArr buffer = new CharArr(32767);
		
		JSONWriter w =new de.avgl.c3.solrWriter.JSONWriter(buffer,-1);
		w.write(map);
		
		return buffer.toString();
	}

}


