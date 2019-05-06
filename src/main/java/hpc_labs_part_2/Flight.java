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

import static org.apache.spark.sql.types.DataTypes.DoubleType;
import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.LongType;
import static org.apache.spark.sql.types.DataTypes.StringType;
import static org.apache.spark.sql.types.DataTypes.createStructField;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

class Flight {
	static StructType schema = DataTypes.createStructType(
			new StructField[]{
	                createStructField("day_of_month", StringType, false),
	                createStructField("day_of_week", StringType, false),
	                createStructField("carrier", StringType, false),
	                createStructField("tailnum", StringType, false),
	                createStructField("flnum", IntegerType, false),
	                createStructField("org_id", LongType, false),
	                createStructField("origin", StringType, false),
	                createStructField("dest_id", LongType, false),
	                createStructField("dest", StringType, false),
	                createStructField("scheduled_dep_time", DoubleType, false),
	                createStructField("dep_time", DoubleType, false),
	                createStructField("departure_delay", DoubleType, false),
	                createStructField("scheduler_arr_time", DoubleType, false),
	                createStructField("arr_time", DoubleType, false),
	                createStructField("arrival_delay", DoubleType, false),
	                createStructField("elapsed_time", DoubleType, false),
	                createStructField("distance", IntegerType, false),	                
	        });
	
	static Row parseFlight(String line) {
		String[] data = line.split(",");
		
		return RowFactory.create(data[0], data[1], data[2], data[3], Integer.parseInt(data[4]), Long.parseLong(data[5]),
				data[6], Long.parseLong(data[7]), data[8], Double.parseDouble(data[9]), Double.parseDouble(data[10]),
				Double.parseDouble(data[11]), Double.parseDouble(data[12]),
				data[13].equals("") ? 0 : Double.parseDouble(data[13]),
				data[14].equals("") ? 0 : Double.parseDouble(data[14]), Double.parseDouble(data[15]),
				Integer.parseInt(data[16]));
	}
	 
	static ExpressionEncoder<Row>  encoder() {
		return RowEncoder.apply(schema);
	}
}