package com.spark.streaming.test;

import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.spark.streaming.StockDetails;

import scala.Tuple2;

public class TestStreamingApp {

	private static final String HOST = "localhost";
	private static final int PORT = 9999;

	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("TestStreamingApp");

		JavaStreamingContext streamingContext = new JavaStreamingContext(conf, Durations.seconds(60));
		Logger.getRootLogger().setLevel(Level.ERROR);

		JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream(HOST, PORT);
		lines.print();
		
		JavaDStream<List<StockDetails>> stocksList = lines.map(f -> { return new ObjectMapper().readValue(f, new TypeReference<List<StockDetails>>() {});});

		JavaDStream<StockDetails> stocks = stocksList.flatMap(new FlatMapFunction<List<StockDetails>, StockDetails>() {

			@Override
			public Iterator<StockDetails> call(List<StockDetails> arg0) throws Exception {
				return arg0.iterator();
			}
		}).filter(f -> f.getSymbol().equals("MSFT"));
		
		
		//DecimalFormat numberFormat = new DecimalFormat("#.0000");
		//JavaPairDStream<String, Double> stockProfitRDD = stocks.window(Durations.seconds(60), Durations.seconds(30)).mapToPair(f -> new Tuple2(f.getSymbol(), numberFormat.format(Double.parseDouble(f.getPriceData().getClose()) - Double.parseDouble(f.getPriceData().getOpen()))));
		JavaPairDStream<String, Double> stockProfitRDD = stocks.window(Durations.seconds(600), Durations.seconds(300)).mapToPair(f -> new Tuple2(f.getSymbol(), Double.parseDouble(f.getPriceData().getClose()) - Double.parseDouble(f.getPriceData().getOpen())));
		JavaPairDStream<String, Double> gains = stockProfitRDD.filter(f -> f._2 > 0).reduceByKey((x,y)-> x + y);
		gains.print();
		JavaPairDStream<String, Double> loses =  stockProfitRDD.filter(f -> f._2 < 0).reduceByKey((x,y)-> Math.abs(x) + Math.abs(y));
		loses.print();
		
		JavaPairDStream<String, Tuple2<Double, Double>> avgProfitAndLoses =  gains.join(loses);
		avgProfitAndLoses.print();
		JavaPairDStream<String, Double> rsi = avgProfitAndLoses.mapValues(new Function<Tuple2<Double,Double>, Double>() {

			@Override
			public Double call(Tuple2<Double, Double> tuple) throws Exception {
				
				Double avgProfit = tuple._1/10.0;
				Double avgLoss =  tuple._2/10.0;
				Double RS = 0.0;
				if(avgLoss != 0){
					RS = avgProfit / avgLoss;
				}
				
				Double RSI = 100 - (100 / (1 + RS));
				
				return RSI;
			}
		});
		
		rsi.print();

		streamingContext.start();
		streamingContext.awaitTermination();
	}
}
