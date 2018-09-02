package com.spark.streaming;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import scala.Tuple2;

public class StockAnalysisV1 {

	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("FirstSparkApplication");
		Logger.getRootLogger().setLevel(Level.ERROR);
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(30));
		jssc.checkpoint("checkpoint_dir");
		JavaDStream<String> stream = jssc.textFileStream("D:/BITS_Pilani_Upgrad_Big_Data/Course_4/Spark_Streaming/Project/freshData").cache();
		
		JavaDStream<List<StockDetails>> stocksList = stream.map(f -> { return new ObjectMapper().readValue(f, new TypeReference<List<StockDetails>>() {});});
		
		JavaDStream<StockDetails> stocks = stocksList.flatMap(f ->  f.stream().collect(Collectors.toList()).iterator());
		
		//JavaPairDStream<String, Long> stockClosing = stocks.mapToPair(f -> new Tuple2(f.getSymbol(), f.getPriceData().getClose()));
		
		JavaPairDStream<String, Long> stockClosing = stocks.window(Durations.minutes(2), Durations.minutes(1)).mapToPair(f -> new Tuple2(f.getSymbol(), Long.parseLong(f.getPriceData().getClose())));
		
		JavaPairDStream<String, Tuple2<Long, Long>> stockClosingCount = stockClosing.mapValues(f -> new Tuple2<Long, Long>(f, 1L));
		
		JavaPairDStream<String, Tuple2<Long, Long>> stockReduceClosingCount = stockClosingCount.reduceByKey((tuple1,tuple2) -> new Tuple2<Long, Long>(tuple1._1 + tuple2._1 , tuple1._2 + tuple2._2));
		
		JavaPairDStream<String, Long> averageRDD = stockReduceClosingCount.mapToPair(new PairFunction<Tuple2<String,Tuple2<Long,Long>>, String, Long>() {

			@Override
			public Tuple2<String, Long> call(Tuple2<String, Tuple2<Long, Long>> tuple) throws Exception {
				Tuple2<Long, Long> var = tuple._2;
				
				long sum = var._1;
				long count = var._2;
				
				return new Tuple2<>(tuple._1, sum/count);
			}
		});
		
		averageRDD.print();
		
		//stockClosing.print();
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
		
	}
}
