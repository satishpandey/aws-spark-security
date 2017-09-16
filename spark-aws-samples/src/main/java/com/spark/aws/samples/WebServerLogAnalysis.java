package com.spark.aws.samples;

import static org.junit.Assert.assertFalse;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

import scala.Tuple2;

import com.spark.aws.samples.beans.ApacheLog;

/**
 * 
 * @author Satish Pandey
 *
 */
public class WebServerLogAnalysis {

	private static final String applicationName = "Web Server Log Analysis";
	private static final Logger logger = LogManager.getLogger(WebServerLogAnalysis.class);
	private static final DateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss");
	private static final String LOG_PATTERN = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+)\\s*(\\S*)\" (\\d{3}) (\\S+)";

	public static void main(String[] args) throws Exception {
		String outputFileLocation = args[0];
		String logFile = "s3a://spark.data.com/NASA_access_log_Jul95.gz";
		SparkConf conf = new SparkConf().setAppName(applicationName);
		JavaSparkContext sc = new JavaSparkContext(conf);
		Configuration configuration = sc.hadoopConfiguration();
		configuration.set("fs.s3a.access.key", "YOUR-AWS-ACCESS-KEY");
		configuration.set("fs.s3a.secret.key", "YOUR-AWS-SECRET-KEY");
		JavaRDD<String> logFileRDD = sc.textFile(logFile);
		JavaRDD<Tuple2<Row, Integer>> logs = logFileRDD.map(prepareLogs);
		logger.info(String.format("Number of failed to parse logs : %d", logs.count()));
		assertFalse("No Data found in RDD", logs.count() == 0);
		JavaRDD<Tuple2<Row, Integer>> failedToParseLogs = logs.filter(new FilterLogs(0));
		logger.info(String.format("Number of failed to parse logs : %d", failedToParseLogs.count()));
		JavaRDD<Tuple2<Row, Integer>> parsedLogsRDD = logs.filter(new FilterLogs(1));
		logger.info(String.format("Number of successful parsed logs : %d", parsedLogsRDD.count()));
		parsedLogsRDD.saveAsTextFile(outputFileLocation);
		sc.close();
		System.exit(0);
	}

	private static Function<String, Tuple2<Row, Integer>> prepareLogs = new Function<String, Tuple2<Row, Integer>>() {
		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Row, Integer> call(String line) throws Exception {
			return parseLogLine(line);
		}
	};

	private static class FilterLogs implements Function<Tuple2<Row, Integer>, Boolean> {
		private static final long serialVersionUID = 1L;
		private int filterValue;

		public FilterLogs(int filterValue) {
			this.filterValue = filterValue;
		}

		@Override
		public Boolean call(Tuple2<Row, Integer> log) throws Exception {
			boolean result = false;
			if (log._2 == filterValue) {
				result = true;
			}
			return result;
		}
	}

	private static Tuple2<Row, Integer> parseLogLine(String line) throws NumberFormatException, ParseException {
		boolean match = line.matches(LOG_PATTERN);
		if (!match) {
			return new Tuple2<Row, Integer>(RowFactory.create(line), 0);
		}
		String[] fields = line.split(LOG_PATTERN);
		ApacheLog log = new ApacheLog(fields[0], fields[1], fields[2], df.parse(fields[3]), fields[4], fields[5],
				fields[6], Integer.parseInt(fields[7]), (fields[8].equals("-") ? 0 : Long.parseLong(fields[8])));
		return new Tuple2<Row, Integer>(RowFactory.create(log), 1);
	}

}