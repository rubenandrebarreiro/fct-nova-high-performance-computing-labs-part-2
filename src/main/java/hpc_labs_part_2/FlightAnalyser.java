package hpc_labs_part_2;

/**
 * High Performance Computing
 * 
 * Practical Lab #3.
 * 
 * Integrated Master of Computer Science and Engineering
 * Faculty of Science and Technology of New University of Lisbon
 * 
 * Authors (Professors):
 * @author Herve Miguel Paulino - herve.paulino@fct.unl.pt
 * 
 * Adapted by:
 * @author Ruben Andre Barreiro - r.barreiro@campus.fct.unl.pt
 *
 */

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class FlightAnalyser {

	private static final String DefaulftFile = "data/flights.csv";

	// 1.1 - Number of Flights Departing From Each Airport Ordered By Alphabetic Ascending of Origin
	
	/**
	 * Returns the number of flights departing from each airport, ordered by alphabetic ascending of Origin field.
	 * 
	 * @param data the data set of the flights read from the .CSV file
	 * 
	 * @return the number of flights departing from each airport, ordered by alphabetic ascending of Origin field
	 */
	public static Dataset<Row> numFlightsDepartingFromEachAirportOrderedByAlphabeticAscOrigin(Dataset<Row> data) {
		 Dataset<Row> result = data.groupBy("origin").count();
		 
		 result.printSchema();

		 result = result.sort(functions.asc("origin"));
		 
		 return result;
	}
	
	/**
	 * Returns the number of flights departing from each airport, ordered by descending of Count field.
	 * 
	 * @param data the data set of the flights read from the .CSV file
	 * 
	 * @return the number of flights departing from each airport, ordered by descending of Count field
	 */
	public static Dataset<Row> numFlightsDepartingFromEachAirportOrderedByDescCount(Dataset<Row> data) {
		 Dataset<Row> result = data.groupBy("origin").count();
		 
		 result.printSchema();
		 
		 result = result.sort(functions.desc("count"));
		 
		 return result;
	}
	
	/**
	 * Returns the number of flights for each route, ordered by alphabetic ascending of Origin field.
	 * 
	 * @param data the data set of the flights read from the .CSV file
	 * 
	 * @return the number of flights for each route, ordered by alphabetic ascending of Origin field
	 */
	public static Dataset<Row> numFlightsForEachRouteOrderedByAlphabeticAscOrigin(Dataset<Row> data) {
		 Dataset<Row> result = data.groupBy("origin", "dest").count();
		 
		 result.printSchema();

		 result = result.sort(functions.asc("origin"));
		 
		 return result;
	}
	
	/**
	 * Returns the number of flights for each route, ordered by alphabetic ascending of Destination field.
	 * 
	 * @param data the data set of the flights read from the .CSV file
	 * 
	 * @return the number of flights for each route, ordered by alphabetic ascending of Destination field
	 */
	public static Dataset<Row> numFlightsForEachRouteOrderedByAlphabeticAscDest(Dataset<Row> data) {
		 Dataset<Row> result = data.groupBy("origin", "dest").count();
		 
		 result.printSchema();

		 result = result.sort(functions.asc("dest"));
		 		 
		 return result;
	}
	
	/**
	 * Returns the number of flights for each route, ordered by descending of Count field.
	 * 
	 * @param data the data set of the flights read from the .CSV file
	 * 
	 * @return the number of flights for each route, ordered by descending of Count field
	 */
	public static Dataset<Row> numFlightsForEachRouteOrderedByDescCount(Dataset<Row> data) {
		 Dataset<Row> result = data.groupBy("origin", "dest").count();
		 
		 result.printSchema();
		 
		 result = result.sort(functions.desc("count"));
		 		 		 
		 return result;
	}
	
	/**
	 * Returns the route with the maximum number of count of flights.
	 * 
	 * @param data the data set of the flights read from the .CSV file
	 * 
	 * @return the route with the maximum number of count of flights
	 */
	public static Dataset<Row> routeWithMaxNumCountOfFlights(Dataset<Row> data) {
		 Dataset<Row> result = data.groupBy("origin", "dest").count();
		 
		 result = result.select("origin", "dest", "count")
				        .where(result.col("count").$eq$eq$eq((result.groupBy().max("count").head()).getLong(0)));
		 		 
		 result.printSchema();
		 
		 return result;
	}
	
	/**
	 * Returns the departure and arrival for each flight.
	 * 
	 * @param data the data set of the flights read from the .CSV file
	 * 
	 * @return the departure and arrival for each flight
	 */
	public static Dataset<Row> departureAndArrivalForEachFlight(Dataset<Row> data) {
		 Dataset<Row> result = data.select("flnum", "origin", "dest", "dep_time", "arr_time");
		 
		 result.printSchema();
		 
		 result.sort(functions.asc("flnum"));
		 
		 return result;
	}
	
	/**
	 * Returns the flight time for each flight.
	 * 
	 * @param data the data set of the flights read from the .CSV file
	 * 
	 * @return the flight time for each flight
	 */
	public static Dataset<Row> flightTimeForEachFlight(Dataset<Row> data) {
		 Dataset<Row> result = data.select("flnum", "origin", "dest", "dep_time", "arr_time");
		 
		 result = result.withColumn("time_of_flight", result.col("arr_time").$minus(result.col("dep_time")));
		 
		 result.printSchema();
		 
		 result = result.sort(functions.asc("flnum"));
		 
		 return result;
	}
	
	/**
	 * Returns the maximum flight time for each flight.
	 * 
	 * @param data the data set of the flights read from the .CSV file
	 * 
	 * @return the maximum flight time for each flight
	 */
	public static Dataset<Row> maxFlightTimeForEachFlight(Dataset<Row> data) {
		 Dataset<Row> result = data.select("flnum", "origin", "dest", "dep_time", "arr_time");
		 
		 result = result.withColumn("time_of_flight", result.col("arr_time").$minus(result.col("dep_time")));
		 
		 result = result.sort(functions.asc("flnum"));
		 
		 result = result.groupBy("flnum", "origin", "dest").max("time_of_flight");
		 
		 result = result.sort(functions.asc("flnum"));
		 
		 result.printSchema();
		 
		 return result;
	}
	
	/**
	 * Returns the maximum flight time of all flights.
	 * 
	 * @param data the data set of the flights read from the .CSV file
	 * 
	 * @return the maximum flight time of all flights
	 */
	public static Dataset<Row> maxFlightTimeOfAllFlights(Dataset<Row> data) {
		 Dataset<Row> result = data.select("flnum", "origin", "dest", "dep_time", "arr_time");
		 
		 result = result.withColumn("time_of_flight", result.col("arr_time").$minus(result.col("dep_time")));
		 
		 result = result.sort(functions.asc("flnum"));
		 
		 result = result.groupBy("flnum", "origin", "dest").max("time_of_flight");
		 
		 result = result.select("flnum", "origin", "dest", "max(time_of_flight)")
  			            .where(result.col("max(time_of_flight)").$eq$eq$eq((result.groupBy().max("max(time_of_flight)").head()).getDouble(0)));
		 
		 result.printSchema();
		 
		 return result;
	}
	
	/**
	 * Returns the arrival delays of flights for each route, ordered by alphabetic ascending of Origin field.
	 * 
	 * @param data the data set of the flights read from the .CSV file
	 * 
	 * @return the arrival delays of flights for each route, ordered by alphabetic ascending of Origin field
	 */
	@SuppressWarnings("deprecation")
	public static Dataset<Row> arrivalDelaysForEachRouteByAlphabeticAscOrigin(Dataset<Row> data) {
		 Dataset<Row> result = data.select("origin", "dest", "arrival_delay")
				                   .where(data.col("arrival_delay").$bang$eq$eq(0.0));
		 
		 result.printSchema();
		 
		 result = result.sort(functions.asc("origin"));
		 		 		 
		 return result;
	}
	
	/**
	 * Returns the arrival delays of flights for each route, ordered by alphabetic ascending of Destination field.
	 * 
	 * @param data the data set of the flights read from the .CSV file
	 * 
	 * @return the arrival delays of flights for each route, ordered by alphabetic ascending of Destination field
	 */
	@SuppressWarnings("deprecation")
	public static Dataset<Row> arrivalDelaysForEachRouteByAlphabeticAscDest(Dataset<Row> data) {
		 Dataset<Row> result = data.select("origin", "dest", "arrival_delay")
				 				   .where(data.col("arrival_delay").$bang$eq$eq(0.0));
		 
		 result.printSchema();
		 
		 result = result.sort(functions.asc("dest"));
		 		 		 
		 return result;
	}
	
	/**
	 * Returns the average of arrival delays of flights for each route, ordered by alphabetic ascending of Origin field.
	 * 
	 * @param data the data set of the flights read from the .CSV file
	 * 
	 * @return the average of arrival delays of flights for each route, ordered by alphabetic ascending of Origin field
	 */
	@SuppressWarnings("deprecation")
	public static Dataset<Row> arrivalDelaysAverageForEachRouteByAlphabeticAscOrigin(Dataset<Row> data) {
		 Dataset<Row> result = data.select("origin", "dest", "arrival_delay")
				 				   .where(data.col("arrival_delay").$bang$eq$eq(0.0));
		 
		 result = result.groupBy("origin", "dest").avg("arrival_delay");
		 
		 result.printSchema();
		 
		 result = result.sort(functions.asc("origin"));
		 		 		 
		 return result;
	}
	
	/**
	 * Returns the average of arrival delays of flights for each route, ordered by alphabetic ascending of Destination field.
	 * 
	 * @param data the data set of the flights read from the .CSV file
	 * 
	 * @return the average of arrival delays of flights for each route, ordered by alphabetic ascending of Destination field
	 */
	@SuppressWarnings("deprecation")
	public static Dataset<Row> arrivalDelaysAverageForEachRouteByAlphabeticAscDest(Dataset<Row> data) {
		 Dataset<Row> result = data.select("origin", "dest", "arrival_delay")
				 				   .where(data.col("arrival_delay").$bang$eq$eq(0.0));
		 
		 result = result.groupBy("origin", "dest").avg("arrival_delay");
		 
		 result.printSchema();
		 
		 result = result.sort(functions.asc("dest"));
		 		 		 
		 return result;
	}
	
	/**
	 * Returns the average of arrival delays of flights for each route, ordered by descending of Average field.
	 * 
	 * @param data the data set of the flights read from the .CSV file
	 * 
	 * @return the average of arrival delays of flights for each route, ordered by descending of Average field
	 */
	@SuppressWarnings("deprecation")
	public static Dataset<Row> arrivalDelaysAverageForEachRouteByDescAvg(Dataset<Row> data) {
		 Dataset<Row> result = data.select("origin", "dest", "arrival_delay")
				 				   .where(data.col("arrival_delay").$bang$eq$eq(0.0));
		 
		 result = result.groupBy("origin", "dest").avg("arrival_delay");
		 
		 result.printSchema();
		 
		 result = result.sort(functions.desc("avg(arrival_delay)"));
		 		 		 
		 return result;
	}

	/**
	 * Main method to process the flights' file and analyze it.
	 * 
	 * @param args the file of flights to process
	 *        (if no args, uses the file flights.csv, by default)
	 */
	public static void main(String[] args) {
		
		String file = (args.length < 1) ? DefaulftFile : args[0];
		
		// start Spark session (SparkContext API may also be used) 
		// master("local") indicates local execution
		SparkSession spark = SparkSession.builder().
				appName("FlightAnalyser").
				master("local[*]").
				getOrCreate();
		
		// only error messages are logged from this point onward
		// comment (or change configuration) if you want the entire log
		spark.sparkContext().setLogLevel("ERROR");

		
		Dataset<String> textFile = spark.read().textFile(file).as(Encoders.STRING());	
		Dataset<Row> flights = 
				textFile.map((MapFunction<String, Row>) l -> Flight.parseFlight(l), 
				Flight.encoder()).cache();
				
		
		// Labs's Exercises:
		// Starting from the code given in the FlighAnalyser.java file, compute:
		// a. The number of flights departing from each airport (DONE)
		// b. The route with more flights (DONE)
		// c. The longest flight (DONE)
		// d. Average delay at arrival per route (DONE)
		
		// All the information about the flights, organised by the corresponding fields
		for (Row r : flights.collectAsList())
			System.out.println(r);
		
		System.out.println();
		System.out.println();
		
		
		// RESOLUTION OF EXERCISES
		
		int index;
		
		// a. The number of flights departing from each airport 
		
		// a.1 - Number of Flights Departing From Each Airport Ordered By Alphabetic Ascending of Origin
		
		System.out.println("Number of Flights Departing From Each Airport Ordered By Alphabetic Ascending of Origin:");
		
		System.out.println();
		
		Dataset<Row> numFlightsDepartingFromEachAirportByAscOrigin = numFlightsDepartingFromEachAirportOrderedByAlphabeticAscOrigin(flights);
		
		index = 1;
		for (Row r : numFlightsDepartingFromEachAirportByAscOrigin.collectAsList())
			System.out.println((index++) + ": " + r);
		
		System.out.println();
		
		// a.2 - Number of Flights Departing From Each Airport Ordered By Descending Count
		
		System.out.println("Number of Flights Departing From Each Airport Ordered By Descending Count:");
		
		System.out.println();
		
		Dataset<Row> numFlightsDepartingFromEachAirportByDescCount = numFlightsDepartingFromEachAirportOrderedByDescCount(flights);
		
		index = 1;
		for (Row r : numFlightsDepartingFromEachAirportByDescCount.collectAsList())
			System.out.println((index++) + ": " + r);

		System.out.println();
		
		
		// b. The route with more flights
		
		// b.1 - Number of Flights For Each Route Ordered By Alphabetic Ascending Origin
		
		System.out.println("Number of Flights For Each Route Ordered By Alphabetic Ascending Origin:");
		
		System.out.println();
		
		Dataset<Row> numFlightsForEachRouteOrderedByAlphabeticAscOrigin =  numFlightsForEachRouteOrderedByAlphabeticAscOrigin(flights);
		
		index = 1;
		for (Row r : numFlightsForEachRouteOrderedByAlphabeticAscOrigin.collectAsList())
			System.out.println((index++) + ": " + r);

		System.out.println();
		
		// b.2 - Number of Flights For Each Route Ordered By Alphabetic Ascending Destination
		
		System.out.println("Number of Flights For Each Route Ordered By Alphabetic Ascending Destination:");
		
		System.out.println();
		
		Dataset<Row> numFlightsForEachRouteOrderedByAlphabeticAscDest = numFlightsForEachRouteOrderedByAlphabeticAscDest(flights);
		
		index = 1;
		for (Row r : numFlightsForEachRouteOrderedByAlphabeticAscDest.collectAsList())
			System.out.println((index++) + ": " + r);
		
		System.out.println();
		
		// b.3 - Number of Flights For Each Route Ordered By Alphabetic Descending Count
		
		System.out.println("Number of Flights For Each Route Ordered By Descending Count:");
				
		System.out.println();
				
		Dataset<Row> numFlightsForEachRouteOrderedByDescCount = numFlightsForEachRouteOrderedByDescCount(flights);
				
		index = 1;
		for (Row r : numFlightsForEachRouteOrderedByDescCount.collectAsList())
			System.out.println((index++) + ": " + r);
		
		System.out.println();
				
		// b.4 - The Route with the Maximum Number of Flights

		System.out.println("The Route with the Maximum Number of Flights:");
				
		System.out.println();
				
		Dataset<Row> routeWithMaxNumCountOfFlights = routeWithMaxNumCountOfFlights(flights);
				
		index = 1;
		for (Row r : routeWithMaxNumCountOfFlights.collectAsList())
			System.out.println((index++) + ": " + r);
		
		System.out.println();

		
		// c. The longest flight
		
		// c.1 - The Departure and Arrival Times For Each Flight

		System.out.println("The Departure and Arrival Times for Each Flight:");
				
		System.out.println();
				
		Dataset<Row> departureAndArrivalForEachFlight = departureAndArrivalForEachFlight(flights);
				
		index = 1;
		for (Row r : departureAndArrivalForEachFlight.collectAsList())
			System.out.println((index++) + ": " + r);
		
		System.out.println();
		
		// c.2 - The Flight's Time For Each Flight

		System.out.println("The Flight's Time for Each Flight:");
				
		System.out.println();
				
		Dataset<Row> flightTimeForEachFlight = flightTimeForEachFlight(flights);
				
		index = 1;
		for (Row r : flightTimeForEachFlight.collectAsList())
			System.out.println((index++) + ": " + r);
		
		System.out.println();

		// c.3 - The Maximum Flight's Time For Each Flight

		System.out.println("The Maximum Flight's Time for Each Flight:");
				
		System.out.println();
				
		Dataset<Row> maxFlightTimeForEachFlight = maxFlightTimeForEachFlight(flights);
				
		index = 1;
		for (Row r : maxFlightTimeForEachFlight.collectAsList())
			System.out.println((index++) + ": " + r);
		
		System.out.println();

		// c.4 - The Maximum Flight's Time Of All Flights

		System.out.println("The Maximum Flight's Time Of All Flights:");
				
		System.out.println();
				
		Dataset<Row> maxFlightTimeOfAllFlights = maxFlightTimeOfAllFlights(flights);
				
		index = 1;
		for (Row r : maxFlightTimeOfAllFlights.collectAsList())
			System.out.println((index++) + ": " + r);
		
		System.out.println();
		
		// d. Average delay at arrival per route
		
		// d.1 - The Arrival Delays For Each Route Ordered By Alphabetic Ascending Origin

		System.out.println("The Arrival Delays For Each Route Ordered By Alphabetic Ascending Origin:");
				
		System.out.println();
				
		Dataset<Row> arrivalDelaysForEachRouteByAlphabeticAscOrigin = arrivalDelaysForEachRouteByAlphabeticAscOrigin(flights);
				
		index = 1;
		for (Row r : arrivalDelaysForEachRouteByAlphabeticAscOrigin.collectAsList())
			System.out.println((index++) + ": " + r);
		
		System.out.println();

		// d.2 - The Arrival Delays For Each Route Ordered By Alphabetic Ascending Destination

		System.out.println("The Arrival Delays For Each Route Ordered By Alphabetic Ascending Destination:");
				
		System.out.println();
				
		Dataset<Row> arrivalDelaysForEachRouteByAlphabeticAscDest = arrivalDelaysForEachRouteByAlphabeticAscDest(flights);
				
		index = 1;
		for (Row r : arrivalDelaysForEachRouteByAlphabeticAscDest.collectAsList())
			System.out.println((index++) + ": " + r);
		
		System.out.println();
		
		// d.3 - The Arrival Delays' Average For Each Route Ordered By Alphabetic Ascending Origin

		System.out.println("The Arrival Delays' Average For Each Route Ordered By Alphabetic Ascending Origin:");
				
		System.out.println();
				
		Dataset<Row> arrivalDelaysAverageForEachRouteByAlphabeticAscOrigin = arrivalDelaysAverageForEachRouteByAlphabeticAscOrigin(flights);
				
		index = 1;
		for (Row r : arrivalDelaysAverageForEachRouteByAlphabeticAscOrigin.collectAsList())
			System.out.println((index++) + ": " + r);
		
		System.out.println();

		// d.4 - The Arrival Delays' Average For Each Route Ordered By Alphabetic Ascending Destination

		System.out.println("The Arrival Delays' Average For Each Route Ordered By Alphabetic Ascending Destination:");
				
		System.out.println();
				
		Dataset<Row> arrivalDelaysAverageForEachRouteByAlphabeticAscDest = arrivalDelaysAverageForEachRouteByAlphabeticAscDest(flights);
				
		index = 1;
		for (Row r : arrivalDelaysAverageForEachRouteByAlphabeticAscDest.collectAsList())
			System.out.println((index++) + ": " + r);
		
		// d.5 - The Arrival Delays' Average For Each Route Ordered By Alphabetic Descending Average

		System.out.println("The Arrival Delays' Average For Each Route Ordered By Descending Average:");
				
		System.out.println();
				
		Dataset<Row> arrivalDelaysAverageForEachRouteByDescAvg = arrivalDelaysAverageForEachRouteByDescAvg(flights);
				
		index = 1;
		for (Row r : arrivalDelaysAverageForEachRouteByDescAvg.collectAsList())
			System.out.println((index++) + ": " + r);
		
		System.out.println();
		
		// Terminate the Spark Session
		spark.stop();
	}
}