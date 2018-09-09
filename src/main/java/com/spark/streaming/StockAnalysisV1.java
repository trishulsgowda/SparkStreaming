package com.spark.streaming;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
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
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(60));
		jssc.checkpoint("checkpoint_dir");
		JavaDStream<String> stream = jssc.textFileStream("D:/BITS_Pilani_Upgrad_Big_Data/Course_4/Spark_Streaming/Project/freshData").cache();

		JavaDStream<List<StockDetails>> stocksList = stream.map(f -> { return new ObjectMapper().readValue(f, new TypeReference<List<StockDetails>>() {});});

		JavaDStream<StockDetails> stocks = stocksList.flatMap(new FlatMapFunction<List<StockDetails>, StockDetails>() {

			@Override
			public Iterator<StockDetails> call(List<StockDetails> arg0) throws Exception {
				return arg0.iterator();
			}
		});


		//JavaPairDStream<String, Double> stockClosing = stocks.window(Durations.minutes(10), Durations.minutes(5)).mapToPair(f -> new Tuple2(f.getSymbol(), Double.parseDouble(f.getPriceData().getClose())));

		/*
		 * Moving average Solution
		 */
		/*JavaPairDStream<String, Tuple2<Double, Double>> stockClosingCount = stockClosing.mapValues(f -> new Tuple2<Double, Double>(f, 1.0));

		JavaPairDStream<String, Tuple2<Double, Double>> stockReduceClosingCount = stockClosingCount.reduceByKey((tuple1,tuple2) -> new Tuple2<Double, Double>(tuple1._1 + tuple2._1 , tuple1._2 + tuple2._2));

		JavaPairDStream<String, Double> averageRDD = stockReduceClosingCount.mapToPair(new PairFunction<Tuple2<String,Tuple2<Double,Double>>, String, Double>() {

			@Override
			public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Double>> tuple) throws Exception {
				Tuple2<Double, Double> var = tuple._2;

				Double sum = var._1;
				Double count = var._2;

				return new Tuple2<>(tuple._1, sum/count);
			}
		});
		averageRDD.print();*/
		
		
		
		JavaPairDStream<String, Double> stockProfit = stocks.window(Durations.minutes(10), Durations.minutes(5)).mapToPair(f -> new Tuple2(f.getSymbol(), Double.parseDouble(f.getPriceData().getClose()) - Double.parseDouble(f.getPriceData().getOpen())));
		//stockProfit.foreachRDD(rdd -> rdd.saveAsTextFile("D:/BITS_Pilani_Upgrad_Big_Data/Course_4/Spark_Streaming/Project/opt"));
		JavaPairDStream<String, Tuple2<Double, Double>> stockProfitCount = stockProfit.mapValues(f -> new Tuple2<Double, Double>(f, 1.0));

		JavaPairDStream<String, Tuple2<Double, Double>> stockReduceProfitCount = stockProfitCount.reduceByKey((tuple1,tuple2) -> new Tuple2<Double, Double>(tuple1._1 + tuple2._1 , tuple1._2 + tuple2._2));
		//stockReduceProfitCount.foreachRDD(rdd -> rdd.saveAsTextFile("D:/BITS_Pilani_Upgrad_Big_Data/Course_4/Spark_Streaming/Project/opt2"));
		JavaPairDStream<String, Double> averageProfitRDD = stockReduceProfitCount.mapToPair(new PairFunction<Tuple2<String,Tuple2<Double,Double>>, String, Double>() {

			@Override
			public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Double>> tuple) throws Exception {
				Tuple2<Double, Double> var = tuple._2;

				Double sum = var._1;
				Double count = var._2;

				return new Tuple2<>(tuple._1, sum/count);
			}
		});
		
		JavaPairDStream<Double, String> profitRDD = averageProfitRDD.mapToPair(f -> new Tuple2(f._2, f._1));
		
		profitRDD.print();
		
		JavaDStream<Tuple2<Double,String>> maxProfitRDD = profitRDD.reduce((t1,t2) -> {
			if(t1._1 > t2._1) return t1; 
			else return t2;
		});
		
		maxProfitRDD.print();
		
		jssc.start();
		jssc.awaitTermination();
		jssc.close();

	}
}
