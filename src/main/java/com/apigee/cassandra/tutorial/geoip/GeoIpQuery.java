package com.apigee.cassandra.tutorial.geoip;

import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.query.SliceQuery;

import com.apigee.cassandra.tutorial.CookbookBase;

public class GeoIpQuery extends CookbookBase {
	
	private Logger logger = LoggerFactory.getLogger(GeoIpQuery.class);
	
	// this is the first component of the Composite for which we will look
	  private static long startArg = 16859136;
	  
	  public static void main(String []args) {
	    init(true);
	    // TODO add maybeCreate() abstract class to tutorial schema
	    GeoIpQuery compositeQuery = new GeoIpQuery();

	    // Note the use of 'equal' and 'greater-than-equal' for the start and end.
	    // this has to be the case when we want all 
	    Composite start = compositeFrom(startArg, Composite.ComponentEquality.EQUAL);
	    Composite end = compositeFrom(3233729536L, Composite.ComponentEquality.GREATER_THAN_EQUAL);

	    compositeQuery.printColumnsFor(start,end);

	  }

	  /**
	   * Prints out the columns we found with a summary of how many there were
	   *
	   * @param start
	   * @param end
	   */
	  public void printColumnsFor(Composite start, Composite end) {

	    CompositeQueryIterator iter = new CompositeQueryIterator(GeoIpCsvLoader.COMPOSITE_KEY, start, end);
	    logger.info("Printing all columns starting with {}", startArg);
	    int count = 0;
	    for ( HColumn<Composite,Composite> column : iter ) {

	      logger.info("Country code: {}  Admin Code: {}   Timezone: {} ",
	    		  new Object[]{
	        column.getName().get(0,LongSerializer.get()),
	        column.getName().get(1,LongSerializer.get()),	        
	        column.getValue().get(0,StringSerializer.get())
	      });
	      count++;
	    }
	    logger.info("Found {} columns",count);
	  }
	    

	  /**
	   * Encapsulates the creation of Composite to make it easier to experiment with values
	   * 
	   * @param componentName
	   * @param equalityOp
	   * @return
	   */
	  public static Composite compositeFrom(Object componentName, Composite.ComponentEquality equalityOp) {
	    Composite composite = new Composite();
	    composite.addComponent(0, componentName, equalityOp);
	    return composite;
	  }

	  /**
	   * Demonstrates the use of Hector's ColumnSliceIterator for "paging" automatically over the results
	   *
	   */
	  class CompositeQueryIterator implements Iterable<HColumn<Composite,Composite>> {

	    private final String key;
	    private final ColumnSliceIterator<String,Composite,Composite> sliceIterator;
	    private Composite start;
	    private Composite end;

	    CompositeQueryIterator(String key, Composite start, Composite end) {
	      this.key = key;
	      this.start = start;
	      this.end = end;

	      SliceQuery<String,Composite,Composite> sliceQuery =
	        HFactory.createSliceQuery(tutorialKeyspace, StringSerializer.get(), CompositeSerializer.get(), CompositeSerializer.get());
	      sliceQuery.setColumnFamily(GeoIpCsvLoader.CF_COMPOSITE_INDEX);
	      sliceQuery.setKey(key);

	      sliceIterator = new ColumnSliceIterator(sliceQuery, start, end, false);
	      // NOTE: view all rows:
	      //sliceIterator = new ColumnSliceIterator(sliceQuery, new Composite(), new Composite(), false);

	    }

	    public Iterator<HColumn<Composite, Composite>> iterator() {
	      return sliceIterator;
	    }


	  }
}
