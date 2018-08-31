package com.spark.streaming;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.fasterxml.jackson.databind.ObjectMapper;

public class StockAnalysisV1 {

	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("FirstSparkApplication");
		Logger.getRootLogger().setLevel(Level.ERROR);
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.minutes(1));
		jssc.checkpoint("checkpoint_dir");
		Thread.sleep(10000);
		JavaDStream<String> stream = jssc.textFileStream("D:/BITS_Pilani_Upgrad_Big_Data/Course_4/Spark_Streaming/Project/try").cache();
		
		JavaDStream<Stocks> stocks = stream.map(f -> {
			return new ObjectMapper().readValue(f, Stocks.class);
		});
		System.out.println(stocks.toString());
		stream.print();
		
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
		
	}
}
