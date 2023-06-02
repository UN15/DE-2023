import scala.Tuple2;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import java.io.*;
import java.util.*;
import java.text.SimpleDateFormat;
import java.text.ParseException;
import java.util.regex.Pattern;
public final class UBERStudent20201018 implements Serializable {
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err.println("Usage: UBERStudent20201018 <in-file> <out-file>");
			System.exit(1);
		}
		SparkSession spark = SparkSession
		.builder()
		.appName("UBERStudent20201018")
		.getOrCreate();
		
		JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

		PairFunction<String, String, String> pf = new PairFunction<String, String, String>() {
			public Tuple2<String, String> call(String s) {
				String[] splitLine = s.split(",");
				String day = "MON";
				try{
					day = getDay(splitLine[1]);
				}catch(Exception e){
					e.printStackTrace();
				}

				String k = splitLine[0] + "," + day;
				String v = splitLine[3] + "," + splitLine[2];
				
				return new Tuple2(k,v);
            		}

			protected String getDay(String date1) throws ParseException {

                        	SimpleDateFormat dateFormat = new SimpleDateFormat("MM/dd/yyyy");

                        	Date date2 = dateFormat.parse(date1);
	
                       		Calendar cal = Calendar.getInstance();
                        	cal.setTime(date2);
                        	int d = cal.get(Calendar.DAY_OF_WEEK);
                        	String day = "MON";

                        	switch(d){
                                	case 1:
                                        	day = "SUN";
                                        	break;
                                	case 2:
                                        	day = "MON";
                                        	break;
                                	case 3:
                                        	day = "TUE";
                                        	break;
                                	case 4:
                                        	day = "WED";
                                        	break;
                                	case 5:
                                        	day = "THR";
                                        	break;
                                	case 6:
                                        	day = "FRI";
                                       	 	break;
                                	case 7:
                                        	day = "SAT";
                                        	break;
                        	}

                        	return day;

                	}

        	};
       		
		JavaPairRDD<String, String> ubers = lines.mapToPair(pf);
        	
		Function2<String, String, String> f2 = new Function2<String, String, String>() {
			public String call(String x, String y) {

				StringTokenizer st = new StringTokenizer(x, ",");
				long trip1 = Long.parseLong(st.nextToken());
				long vehicle1 = Long.parseLong(st.nextToken());

				StringTokenizer st2 = new StringTokenizer(y, ",");
				long trip2 = Long.parseLong(st2.nextToken());
				long vehicle2 = Long.parseLong(st2.nextToken());
				
				long trips = trip1+trip2;
				long vehicles = vehicle1+vehicle2;
				String result = trips+","+vehicles;
				return result;
            		}
        	};
        	
		JavaPairRDD<String, String> counts = ubers.reduceByKey(f2);

		counts.saveAsTextFile(args[1]);
        	spark.stop();
	
	}
}
