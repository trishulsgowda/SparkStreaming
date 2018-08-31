package com.spark.streaming;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Batch {
	public static void main(String[] args) throws JsonParseException, JsonMappingException, IOException {
		//SparkConf conf = new SparkConf().setMaster("local").setAppName("wordcount");
		//JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
		
		//JavaRDD<String> inputData = javaSparkContext.textFile("D:/BITS_Pilani_Upgrad_Big_Data/Course_4/Spark_Streaming/Project/try");
		
		String json = "[{\"symbol\": \"MSFT\", \"timestamp\": \"2018-08-29 13:08:00\", \"priceData\": {\"open\": \"111.6200\", \"high\": \"111.6200\", \"low\": \"111.6050\", \"close\": \"111.6200\", \"volume\": \"18254\"}}, {\"symbol\": \"ADBE\", \"timestamp\": \"2018-08-29 13:09:00\", \"priceData\": {\"open\": \"268.5600\", \"high\": \"268.5600\", \"low\": \"268.4300\", \"close\": \"268.5100\", \"volume\": \"6579\"}}, {\"symbol\": \"GOOGL\", \"timestamp\": \"2018-08-29 13:09:00\", \"priceData\": {\"open\": \"1263.6436\", \"high\": \"1263.6436\", \"low\": \"1263.4100\", \"close\": \"1263.4100\", \"volume\": \"625\"}}, {\"symbol\": \"FB\", \"timestamp\": \"2018-08-29 13:09:00\", \"priceData\": {\"open\": \"175.5600\", \"high\": \"175.5753\", \"low\": \"175.5100\", \"close\": \"175.5100\", \"volume\": \"8997\"}}]";
		
		ObjectMapper mapper = new ObjectMapper();
		List<StockDetails> stocks = mapper.readValue(json, new TypeReference<List<StockDetails>>() {}) ;
		
		for(StockDetails s : stocks){
			System.out.println(s.getSymbol());
		}
		
		
		/*JavaRDD<String> sym = stocks.flatMap(new FlatMapFunction<Stocks, String>() {

			@Override
			public Iterator<String> call(Stocks stock) throws Exception {
				List<String> list = new ArrayList<>();
				List<StockDetails> sd = stock.getStockDetails();
				for(StockDetails sd1 : sd){
					list.add(sd1.getSymbol());
				}
				return list.iterator();
			}
		});
		
		sym.saveAsTextFile("C:/Users/Thrishul/Desktop/test1.txt");*/
	
	}
}
