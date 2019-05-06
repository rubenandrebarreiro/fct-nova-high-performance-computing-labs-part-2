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

import java.util.Arrays;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class WordCount {

	private static final String DefaulftFile = "data/hamlet.txt";
	
	
	public static Dataset<Row> wordCount(Dataset<String> data) {

		 Dataset<String> words = data.flatMap(
				 	(FlatMapFunction<String, String>) s ->  Arrays.asList(s.toLowerCase().split(" ")).iterator(), 
				 	Encoders.STRING());
		 
		 words.printSchema();
		 
		 Dataset<Row> result = words.groupBy("value").count();
		 result.printSchema();
		 		 
		return result.sort(functions.desc("count"));
		 
	}
	
	
	public static void main(String[] args) {
		
		String file = (args.length < 1)  ?
				DefaulftFile : args[0];

		// Start Spark session (SparkContext API may also be used) 
		// Master("local") indicates local execution
		SparkSession spark = SparkSession.builder().
				appName("WordCount").master("local").getOrCreate();
		
		// Only error messages are logged from this point onward
		// Comment (or change configuration) if you want the entire log
		spark.sparkContext().setLogLevel("ERROR");

		Dataset<String> textFile = spark.read().textFile(file).as(Encoders.STRING());
		Dataset<Row> counts =  wordCount(textFile);

		// Print all as list
		// System.out.println(counts.collectAsList());
		
		int i = 1;
		for (Row r : (Row[]) counts.take(20))
			System.out.println((i++) + ": " + r);
	
		// Terminate the Spark Session
		spark.stop();
	}
}