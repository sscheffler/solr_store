package de.tdb.main;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.tdb.TDBFactory;

public class TDBMain {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Dataset set = TDBFactory.createDataset("/home/stefan/stores/ts/");
		
		Model m = set.getDefaultModel();
		Resource r1 = m.createResource("http://de.test/a");
		Resource r2 = m.createResource("http://de.test/b");
		Resource r3 = m.createResource("http://de.test/c");
		
		Property p1 = m.createProperty("http://de.test/p1");
		Property p2 = m.createProperty("http://de.test/p2");
		Property p3 = m.createProperty("http://de.test/p3");
		
		m.add(r1, p1, "prop1");
		m.add(r2, p2, "prop2");
		m.add(r3, p3, "prop3");
		
		m.commit();
		set.close();
		

	}

}
