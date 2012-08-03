package de.test.respWriter;

import java.io.IOException;
import java.io.Writer;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.document.Document;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.QueryResponseWriter;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.response.TextResponseWriter;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.SolrIndexReader;

import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.tdb.TDBFactory;

public class CustomResponseWriter implements QueryResponseWriter {

	@Override
	public void write(Writer writer, SolrQueryRequest request,
			SolrQueryResponse response) throws IOException {
		Model model = TDBFactory.createDataset("/home/stefan/stores/ts/").getDefaultModel();
		
		SolrParams params = request.getParams();	
  	  	
  	  	
  	  	/*if(!sparqlQuery.contains("?tsID")){
  		  writer.write("Sparql does not contain '?tsId'");
  		  return;
  	  	}*/
		
			
		NamedList lst = response.getValues();
	    Boolean omitHeader = request.getParams().getBool(CommonParams.OMIT_HEADER);
	    if(omitHeader != null && omitHeader) lst.remove("responseHeader");
	    int sz = lst.size();
	    int start=0;
	    
	    SolrIndexReader reader = request.getSearcher().getReader();
	    
	    for (int i=start; i<sz; i++) {
	    	
	    	if(lst.getVal(i) instanceof DocSlice){
	    		DocSlice slice = (DocSlice)lst.getVal(i);
	    		
	    		SolrDocumentList rl = new SolrDocumentList();
	    		for (DocIterator it = slice.iterator(); it.hasNext(); ){
	    			Document doc = reader.document(it.nextDoc());
	    	    	String[] idArray=doc.getValues("tsId");
//	    	    	System.out.println(idArray);
	    	    	if(idArray.length == 0){
	    	    		writer.write("Field 'tsId' is NULL");
	    	    		return;
	    	    	}
	    	    	  
	    	    	String id = idArray[0];
	    	    	System.out.println(id);
	    	    	String sparqlQuery = params.get("sparql");
	    	    	
	    	    	if(sparqlQuery == null){
	    	    		  writer.write("Parameter 'sparql' is not set");
	    	    		  return;
	    	    	  	}
	    	    	
	    	    	sparqlQuery = sparqlQuery.replaceAll("\\?tsId", id);
	    	    	 
	    	    	System.out.println(sparqlQuery);
	    	    	
	    	    	Query query = QueryFactory.create(sparqlQuery) ;
	    	    	QueryExecution qexec = QueryExecutionFactory.create(query, model) ;
	    	    	try{
	    	    		ResultSet results = qexec.execSelect() ;
	    			    
	    			    //String str =ResultSetFormatter.asText(results);
	    			    ResultSetFormatter.outputAsJSON(System.out, results);
	    	    		  
	    	    	  }finally{qexec.close();}
	    	    	  
	    	    	  
	    	      }
	    		
	    		
	    		
	    		
	    	}
	      }
	    
	    
		
		
		
		//String sparqlQuery = p.get("sparql");
		//sparqlQuery.replaceAll("tsId", tsId);
		//System.out.println(sparqlQuery);
		
//		String queryString = " Select ?y ?z where { <http://de.test/a> ?y ?z } " ;
		//  Query query = QueryFactory.create(sparqlQuery) ;
		//  QueryExecution qexec = QueryExecutionFactory.create(query, model) ;
		
	//	  try {
	//	    ResultSet results = qexec.execSelect() ;
		    
		    //String str =ResultSetFormatter.asText(results);
		//    ResultSetFormatter.outputAsJSON(System.out, results);
		    //writer.write(str);
//		    JSONOutputResultSet set = new JSONOutputResultSet(System.out);
		    /*for ( ; results.hasNext() ; )
		    {
		      QuerySolution soln = results.nextSolution() ;
		      RDFNode x = soln.get("varName") ;       // Get a result variable by name.
		      Resource r = soln.getResource("VarR") ; // Get a result variable - must be a resource
		      Literal l = soln.getLiteral("VarL") ;   // Get a result variable - must be a literal
		    }*/
	//	  } finally { qexec.close() ; }
		
		

	}

	@Override
	public String getContentType(SolrQueryRequest request,
			SolrQueryResponse response) {
		// TODO Auto-generated method stub
		return CONTENT_TYPE_TEXT_UTF8;
	}

	@Override
	public void init(NamedList args) {
		// TODO Auto-generated method stub

	}

}


class OwnWriter extends TextResponseWriter{

	public OwnWriter(Writer writer, SolrQueryRequest req, SolrQueryResponse rsp) {
		super(writer, req, rsp);
		
	}
	
	public void writeResponse(){
		
	}

	@Override
	public void writeNamedList(String name, NamedList val) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeDoc(String name, Document doc, Set<String> returnFields,
			float score, boolean includeScore) throws IOException {
		System.out.println("WriteDoc: " + returnFields + " - " + name);
		
	}

	@Override
	public void writeSolrDocument(String name, SolrDocument doc,
			Set<String> returnFields, Map pseudoFields) throws IOException {
		System.out.println("SolrDoc: " + returnFields + " - " +name);
		
	}

	@Override
	public void writeDocList(String name, DocList ids, Set<String> fields,
			Map otherFields) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeSolrDocumentList(String name, SolrDocumentList docs,
			Set<String> fields, Map otherFields) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeStr(String name, String val, boolean needsEscaping)
			throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeMap(String name, Map val, boolean excludeOuter,
			boolean isFirstVal) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeArray(String name, Object[] val) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeArray(String name, Iterator val) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeNull(String name) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeInt(String name, String val) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeLong(String name, String val) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeBool(String name, String val) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeFloat(String name, String val) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeDouble(String name, String val) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeDate(String name, String val) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeShort(String name, String val) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeByte(String name, String s) throws IOException {
		// TODO Auto-generated method stub
		
	}
	
}