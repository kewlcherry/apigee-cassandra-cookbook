# Cassandra Cookbook for Apigee

This is designed to be a series of example, largely standalone applications for common tasks in working with Apache Cassandra.

Currently, examples assume you have a stock instance of Apache Cassandra 1.0.x running with the default configuration (thrift listening on localhost:9160) and are using maven (version 3.0.x). 

Examples are run individually via the CLI and maven-exec-plugin. How to run a specific class should be in the Javadoc at the top of the file. For example, run the insert and query classes for the GeoIP example, do the following:

	`mvn -e exec:java -Dexec.mainClass="com.apigee.cassandra.tutorial.geoip.GeoIpCsvLoader"`
	
which will load the data, then:

	`mvn -e exec:java -Dexec.mainClass="com.apigee.cassandra.tutorial.geoip.GeoIpQuery"`

which will print out the first 4 rows of sample data.