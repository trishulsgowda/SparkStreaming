package com.spark.streaming;
import java.util.Arrays;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


public class FirstSparkApplication {

	public static void main(String[] args) throws InterruptedException {
		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("FirstSparkApplication");
		Logger.getRootLogger().setLevel(Level.ERROR);
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
		jssc.checkpoint("checkpoint_dir");
		JavaReceiverInputDStream<String> lines =jssc.socketTextStream("localhost", 9999); // nc -L -p 9999
		/*lines.flatMap(f -> Arrays.asList(f.split(" ")).iterator())
				.mapToPair(f -> new Tuple2<>(f, 1))
				.updateStateByKey((values,currentState) -> {
					int sum = (int) currentState.or(0);
					for(int i : values){
						sum += i;
					}
					return Optional.of(sum);
				})
				.print();*/

		/*lines.window(Durations.seconds(20), Durations.seconds(10))
			.print();*/

		Function2<String, String, String> reduceFunc =  new Function2<String, String, String>() {

			@Override
			public String call(String arg0, String arg1) throws Exception {
				return "Window is filled";
			}
		};
		
		Function2<String, String, String> invReduceFunc =  new Function2<String, String, String>() {

			@Override
			public String call(String arg0, String arg1) throws Exception {
				return "Window is empty";
			}
		};
		
		lines.flatMap(f -> Arrays.asList(f.split(" ")).iterator())
		.reduceByWindow(reduceFunc, invReduceFunc,  Durations.seconds(10),  Durations.seconds(6))
		.print();
		
		//jssc.start();
		//jssc.awaitTermination();
		//jssc.close();
	}
}
