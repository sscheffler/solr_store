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
	private String tsId="";
	
	
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
	
	private static Map<String, Integer> queryIdentifyMap;
	static{
		queryIdentifyMap = new HashMap<String, Integer>();
		queryIdentifyMap.put("getAllContacts", 1);
		queryIdentifyMap.put("getFuzzyContacts", 2);
		queryIdentifyMap.put("getIdentifiedContacts", 3);
		queryIdentifyMap.put("special1", 4);
	}
	
	/*private static Map<Integer, String> queryStringMap;
	static{
		queryStringMap = new HashMap<Integer, String>();
		queryStringMap.put(1,"");
		queryStringMap.put(2,"");
		queryStringMap.put(3,"");
		queryStringMap.put(4,"");
	}*/
	
	
	@Override
	public void init(NamedList list) {
		tsPath = (String)list.get("storePath");
		model = TDBFactory.createDataset(tsPath).getDefaultModel();
	}
	
	/**
	 * calls the Method for a preset query
	 * @param i
	 * @param query
	 */
	private void call(Integer i/*, String query*/){
		switch (i){
		case 1:{getAllContacts();break;}
		case 2:{getFuzzyContacts();break;}
		case 3:{getIdentifiedContacts();break;}
		case 4:{getSpecial1();break;}
		}
	}
	
	private void getAllContacts(){
		System.out.println("all");
	}
	
	private void getFuzzyContacts(){
		System.out.println("fuzzy");
	}
	
	private void getIdentifiedContacts(){
		System.out.println("ident");
	}
	
	private void getSpecial1(){
		String q = "PREFIX : <http://avantgarde-labs.de/c3/ontology.owl#>"+
				"PREFIX r: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"+
				"PREFIX i: <http://avantgarde-labs.de/c3/resource#>"+
				"Select ?p ?o {{?tsId :hasAddress ?a}{?a ?p ?o} UNION {?tsId ?p ?o} FILTER (?tsId IN (%s) && ?p != :hasAddress && ?p != r:type)}";
		
		
		String query = this.getQuery(this.getTsId(), q);
		System.out.println("special1");
		execQuery(query);
		
	}
	
	
	private void execQuery(String queryString){
		QueryExecution qexec = QueryExecutionFactory.create(queryString, model) ;
    	try{
    		ResultSet results = qexec.execSelect();
//    		ByteArrayOutputStream out = new ByteArrayOutputStream();
		    
		    ResultSetFormatter.outputAsJSON(System.out, results);
		    
    		  
    	}finally{qexec.close();}
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
	    			this.setTsId(doc.get("tsId"));
//	    			String tsId=doc.get("tsId");
	    	    	if(this.getTsId().equals("")){
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
	    	    	String query=getQuery(this.getTsId(), params.get("sparql"));
	    	    	
	    	    	// Preset Query?
	    	    	if(queryIdentifyMap.containsKey(query)){
	    	    		Integer identify = queryIdentifyMap.get(query);
	    	    		call(identify/*, queryStringMap.get(identify)*/);
	    	    	}
	    	    	
	    	    	
	    	    	
	    	    	
	    	  /*  	QueryExecution qexec = QueryExecutionFactory.create(query, model) ;
	    	    	try{
	    	    		ResultSet results = qexec.execSelect();
	    	    		ByteArrayOutputStream out = new ByteArrayOutputStream();
	    			    
	    			    ResultSetFormatter.outputAsJSON(out, results);
	    			    resMap.put(SPARQL_ID,out.toString());
	    	    		  
	    	    	}finally{qexec.close();}
	    	    	((List<Object>)finalJsonMap.get(RESULTS_ID)).add(resMap);*/
	    	    	this.setTsId("");
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

	public String getTsId() {
		return tsId;
	}

	public void setTsId(String tsId) {
		this.tsId = tsId;
	}

}


