package com.spark.streaming;
import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
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
		
		//D:/BITS_Pilani_Upgrad_Big_Data/Course_4/Spark_Streaming/Project/getdata.py
		
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("FirstSparkApplication");
		Logger.getRootLogger().setLevel(Level.ERROR);
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(15));
		jssc.checkpoint("checkpoint_dir");
		//JavaDStream<String> stream = jssc.textFileStream("D:/BITS_Pilani_Upgrad_Big_Data/Course_4/Spark_Streaming/Project/freshData").cache();
		
		JavaDStream<String> stream = jssc.textFileStream(args[0]).cache();
		
		JavaDStream<List<StockDetails>> stocksList = stream.map(f -> { return new ObjectMapper().readValue(f, new TypeReference<List<StockDetails>>() {});});

		JavaDStream<StockDetails> stocks = stocksList.flatMap(new FlatMapFunction<List<StockDetails>, StockDetails>() {

			@Override
			public Iterator<StockDetails> call(List<StockDetails> arg0) throws Exception {
				return arg0.iterator();
			}
		});


		JavaPairDStream<String, Double> stockClosing = stocks.window(Durations.minutes(10), Durations.minutes(5)).mapToPair(f -> new Tuple2(f.getSymbol(), Double.parseDouble(f.getPriceData().getClose())));

		/*
		 * Moving average Solution
		 */
		JavaPairDStream<String, Tuple2<Double, Double>> stockClosingCount = stockClosing.mapValues(f -> new Tuple2<Double, Double>(f, 1.0));

		JavaPairDStream<String, Tuple2<Double, Double>> stockReduceClosingCount = stockClosingCount.reduceByKey((tuple1,tuple2) -> new Tuple2<Double, Double>(tuple1._1 + tuple2._1 , tuple1._2 + tuple2._2));

		JavaPairDStream<String, Double> averageRDD = stockReduceClosingCount.mapToPair(new PairFunction<Tuple2<String,Tuple2<Double,Double>>, String, Double>() {

			public Tuple2<String, Double> call(Tuple2<String, Tuple2<Double, Double>> tuple) throws Exception {
				Tuple2<Double, Double> var = tuple._2;

				Double sum = var._1;
				Double count = var._2;

				return new Tuple2<>(tuple._1, sum/count);
			}
		});
		averageRDD.foreachRDD(f -> {
			String messageId = String.valueOf(System.currentTimeMillis());
			f.coalesce(1).saveAsTextFile(args[1]+"_MovingAverage_" + messageId);
		});
		
		
		/*
		 * Finding the stock with maximum value
		 */
		/*JavaPairDStream<String, Double> stockProfit = stocks.window(Durations.minutes(10), Durations.minutes(5)).mapToPair(f -> new Tuple2(f.getSymbol(), Double.parseDouble(f.getPriceData().getClose()) - Double.parseDouble(f.getPriceData().getOpen())));
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
		
		maxProfitRDD.print();*/
		
		//DecimalFormat numberFormat = new DecimalFormat("#.0000");
		//JavaPairDStream<String, Double> stockProfitRDD = stocks.window(Durations.seconds(30), Durations.seconds(15)).mapToPair(f -> new Tuple2(f.getSymbol(), numberFormat.format(Double.parseDouble(f.getPriceData().getClose()) - Double.parseDouble(f.getPriceData().getOpen()))));
		////JavaPairDStream<String, Double> stockLosesRDD = stockProfitRDD.mapToPair(f -> new Tuple2(f._1, f._2));
		
		/*stockProfitRDD.reduceByKey(new Function2<Double, Double, Double>() {

			@Override
			public Double call(Double arg0, Double arg1) throws Exception {
				// TODO Auto-generated method stub
				return null;
			}
		});*/
		
		//stockProfitRDD.print();
		
		//stockProfitRDD.foreachRDD(rdd -> rdd.saveAsTextFile("D:/BITS_Pilani_Upgrad_Big_Data/Course_4/Spark_Streaming/Project/opt"));
		//stockLosesRDD.foreachRDD(rdd -> rdd.saveAsTextFile("D:/BITS_Pilani_Upgrad_Big_Data/Course_4/Spark_Streaming/Project/opt1"));
		
		//JavaPairDStream<String, Double> gains = stockProfitRDD.filter(f -> f._2 > 0).reduceByKey((x,y)-> x + y);
		
		//JavaPairDStream<String, Double> loses = stockProfitRDD.filter(f -> f._2 < 0).reduceByKey((x,y)-> Math.abs(x) + Math.abs(y));
		
		//gains.foreachRDD(rdd -> rdd.saveAsTextFile("D:/BITS_Pilani_Upgrad_Big_Data/Course_4/Spark_Streaming/Project/opt2"));
		//loses.foreachRDD(rdd -> rdd.saveAsTextFile("D:/BITS_Pilani_Upgrad_Big_Data/Course_4/Spark_Streaming/Project/opt3"));
		
		/*JavaPairDStream<String, Tuple2<Double, Double>> avgProfitAndLoses =  gains.join(loses); 
		
		JavaPairDStream<String, Double> rsi = avgProfitAndLoses.mapValues(new Function<Tuple2<Double,Double>, Double>() {

			@Override
			public Double call(Tuple2<Double, Double> tuple) throws Exception {
				
				Double avgProfit = tuple._1/10.00;
				Double avgLoss =  tuple._2/10.00;
				Double RS = 0.0;
				if(avgLoss != 0){
					RS = avgProfit / avgLoss;
				}
				
				Double RSI = 100 - (100 / (1 + RS));
				
				return RSI;
			}
		});
		
		rsi.print();*/
		
		jssc.start();
		jssc.awaitTermination();
		jssc.close();

	}
}
