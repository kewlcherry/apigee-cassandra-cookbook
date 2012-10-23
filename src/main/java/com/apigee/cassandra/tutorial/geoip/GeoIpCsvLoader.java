package com.apigee.cassandra.tutorial.geoip;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import me.prettyprint.cassandra.model.BasicColumnFamilyDefinition;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.DynamicCompositeSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ThriftCfDef;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.DynamicComposite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.apigee.cassandra.tutorial.CookbookBase;
import com.apigee.cassandra.tutorial.SchemaUtils;

/**
 * Uses maxmind GeoIP data to demonstrate:
 * <li>parallelized insertion</li>
 * <li>Composite indexes</li>
 * <li>Choosing proper key partition for an index table</li>
 * 
 * Note that the use case for this data is to provide a performant lookup of static data.
 * 
 * mvn -e exec:java -Dexec.mainClass="com.apigee.cassandra.tutorial.geoip.GeoIpCsvLoader"
 * 
 * @author zznate 
 */
public class GeoIpCsvLoader extends CookbookBase {

	private static Logger log = LoggerFactory.getLogger(GeoIpCsvLoader.class);

	  private static ExecutorService exec;
	  // key for static composite, First row of dynamic composite
	  public static final String COMPOSITE_KEY = "ALL";
	  public static final String CF_COMPOSITE_INDEX = "CompositeSingleRowIndex";

	  public static void main(String[] args) {
	    long startTime = System.currentTimeMillis();
	    init(true);
	    maybeCreateSchema();
	    //String fileLocation = properties.getProperty("composites.geodata.file.location","data/geodata.txt");
	    String fileLocation = properties.getProperty("composites.geodata.file.location","data/GeoIPCountryWhois.csv");

	    BufferedReader reader;
	    exec = Executors.newFixedThreadPool(5);
	    try {
	      reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileLocation)));
	      // read 1000 and hand off to worker

	      List<String> lines = new ArrayList<String>(1000);
	      String line = reader.readLine();

	      List<Future<Integer>> sums = new ArrayList<Future<Integer>>();
	      while(line != null) {

	        lines.add(line);
	        if ( lines.size() % 250 == 0 ) {
	          doParse(lines, sums);
	        }
	        line = reader.readLine();
	      }
	      doParse(lines, sums);

	      int total = 0;
	      for (Future<Integer> future : sums) {
	        // naive wait for completion
	        total = total + future.get().intValue();
	      }

	      log.info("Inserted a total of {} over duration ms: {}", total, System.currentTimeMillis() - startTime);
	    } catch (Exception e) {
	      log.error("Could not locate file",e);
	    } finally {
	      exec.shutdown();
	    }
	    tutorialCluster.getConnectionManager().shutdown();
	  }


	  protected static void maybeCreateSchema() {
	    BasicColumnFamilyDefinition columnFamilyDefinition = new BasicColumnFamilyDefinition();
	    columnFamilyDefinition.setKeyspaceName(SchemaUtils.COOKBOOK_KEYSPACE_NAME);
	    columnFamilyDefinition.setName(CF_COMPOSITE_INDEX);
	    columnFamilyDefinition.setComparatorType(ComparatorType.COMPOSITETYPE);
	    columnFamilyDefinition.setComparatorTypeAlias("(LongType, LongType)");
	    columnFamilyDefinition.setDefaultValidationClass("CompositeType(UTF8Type,UTF8Type,UTF8Type,UTF8Type)");
	    //columnFamilyDefinition.setDefaultValidationClass("DynamicCompositeType(a=>AsciiType,b=>BytesType,i=>IntegerType,x=>LexicalUUIDType,l=>LongType,t=>TimeUUIDType,s=>UTF8Type,u=>UUIDType,A=>AsciiType(reversed=true),B=>BytesType(reversed=true),I=>IntegerType(reversed=true),X=>LexicalUUIDType(reversed=true),L=>LongType(reversed=true),T=>TimeUUIDType(reversed=true),S=>UTF8Type(reversed=true),U=>UUIDType(reversed=true))");
	    columnFamilyDefinition.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());
	    ColumnFamilyDefinition cfDef = new ThriftCfDef(columnFamilyDefinition);
	    schemaUtils.maybeCreate(cfDef);
	  }

	  private static void doParse(List<String> lines, List<Future<Integer>> sums) {
	    Future<Integer> f = exec.submit(new GeoIpCsvLoader().new LineParser(new ArrayList(lines)));
	    sums.add(f);
	    lines.clear();
	  }


	  class LineParser implements Callable<Integer> {

	    List<String> lines;
	    LineParser(List<String> lines) {
	      this.lines = lines;
	    }

	    public Integer call() throws Exception {
	      int count = 0;
	      GeoDataLine geoDataLine;
	      Mutator<String> mutator = HFactory.createMutator(tutorialKeyspace, StringSerializer.get());

	      for (String row : lines) {
	        // parse
	        geoDataLine = new GeoDataLine(row);
	        // assemble the insertions
	        mutator.addInsertion(COMPOSITE_KEY, CF_COMPOSITE_INDEX, geoDataLine.ipStartNumberFrom());
	        
	        count++;
	      }
	      mutator.execute();
	      log.debug("Inserted {} columns", count);
	      return Integer.valueOf(count);
	    }

	  }

	  /**
	   * This is probably overkill given the simplicity of the data, but is
	   * good practice for separation of concerns and encapsulation
	   */
	  static class GeoDataLine {
	    public static final String SEPARATOR = ",";
	    private String[] vals = new String[10];

	    // columnar format with several example rows:
	    // startAddress, endAddress, startNumber, endNumber, countryCode, countryName	    
	    // 31.204.65.64	31.204.65.95	533479744	533479775	FI	Finland
	    // 78.24.206.248	78.24.206.255	1310248696	1310248703	RU	Russian Federation
	    // 122.102.40.0	122.102.55.255	2053515264	2053519359	ID	Indonesia
	    // 31.204.65.64	31.204.65.95	533479744	533479775	FI	Finland
	    // 217.155.51.96	217.155.255.255	3650827104	3650879487	GB	United Kingdom
	    
	    // store several different ways:
	    // - start number first: IP_START_NUMBER
	    // - end number first: IP_END_NUMBER
	    // - per country code: IP_COUNTRY_CODE
	    
	    GeoDataLine(String line) {
	      vals = StringUtils.split(StringEscapeUtils.unescapeCsv(line), SEPARATOR);
	      log.debug("array size: {} for row: {}", vals.length, line);
	      log.debug("parsed: {} ", this.toString());
	    }

	    /**
	     * Creates an HColumn with a column name composite of the form:
	     *   ['startNum']:['endNum'] 
	     * The value will be: [country code][country name][ip start][ip end]
	     * @return
	     */
	    HColumn<Composite,Composite> ipStartNumberFrom() {

	      Composite name = new Composite();	      
	      name.addComponent(getStartNumber(), LongSerializer.get());
	      name.addComponent(getEndNumber(), LongSerializer.get());
	      
	      Composite value = new Composite();
	      value.addComponent(getCountryCode(), StringSerializer.get());
	      value.addComponent(getCountryName(), StringSerializer.get());
	      value.addComponent(getStartAddress(), StringSerializer.get());
	      value.addComponent(getEndAddress(), StringSerializer.get());

	      
	      HColumn<Composite,Composite> col =	      
	    		 HFactory.createColumn(name, value, CompositeSerializer.get(), CompositeSerializer.get());
	      return col;
	    }


	    String getCountryCode() {
	      return StringUtils.remove(vals[4],"\"");
	    }
	    String getCountryName() {
	    	return StringUtils.remove(vals[5],"\"");

	    }
	    String getStartAddress() {
	    	return StringUtils.remove(vals[0],"\"");
	      
	    }
	    String getEndAddress() {
	    	return StringUtils.remove(vals[1],"\"");
	    }
	    long getStartNumber() {
	    	return NumberUtils.toLong(StringUtils.remove(vals[2],"\""));

	    }
	    long getEndNumber() {
	    	return NumberUtils.toLong(StringUtils.remove(vals[3],"\""));
	    }
	    
	    @Override
	    public String toString() {
	    	return new StringBuilder().append("cc: ").append(getCountryCode())
	    			.append(", name:").append(getCountryName())
	    			.append(", startAdr: ").append(getStartAddress())
	    			.append(", endAddr:").append(getEndAddress())
	    			.append(", startNum:").append(getStartNumber())
	    			.append(", endNum:").append(getEndNumber())
	    			.toString();
	    }
	  }
	}
