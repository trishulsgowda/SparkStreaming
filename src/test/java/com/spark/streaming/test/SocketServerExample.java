package com.spark.streaming.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class SocketServerExample {
	private static final Executor SERVER_EXECUTOR = Executors.newSingleThreadExecutor();
    private static final int PORT = 9999;
    private static final long EVENT_PERIOD_SECONDS = 60;

    public static void main(String[] args) throws IOException, InterruptedException {
        BlockingQueue<String> eventQueue = new ArrayBlockingQueue<>(100);
        SERVER_EXECUTOR.execute(new SteamingServer(eventQueue));
        //while (true) {
          //  eventQueue.put(generateEvent());
            //Thread.sleep(TimeUnit.SECONDS.toMillis(EVENT_PERIOD_SECONDS));
        //}
        File dir = new File("D:/BITS_Pilani_Upgrad_Big_Data/Course_4/Spark_Streaming/Project/freshDataTest");
		File[] directoryListing = dir.listFiles();
		
		if (directoryListing != null) {
			int count = 1;
			for (File child : directoryListing) {
				FileReader fr = new FileReader(child);
				BufferedReader br = new BufferedReader(fr);
				
				String line = null;
				
				while( (line = br.readLine())!= null){
					eventQueue.put(line);
				}
				Thread.sleep(TimeUnit.SECONDS.toMillis(EVENT_PERIOD_SECONDS));
			}
		}
    }

   /* private static String generateEvent() {
    	File dir = new File("D:/BITS_Pilani_Upgrad_Big_Data/Course_4/Spark_Streaming/Project/freshDataTest");
		File[] directoryListing = dir.listFiles();
		
		if (directoryListing != null) {
			int count = 1;
			for (File child : directoryListing) {
				
			}
		}
    }*/

    private static class SteamingServer implements Runnable {
        private final BlockingQueue<String> eventQueue;

        public SteamingServer(BlockingQueue<String> eventQueue) {
            this.eventQueue = eventQueue;
        }

        @Override
        public void run() {
            try (ServerSocket serverSocket = new ServerSocket(PORT);
                 Socket clientSocket = serverSocket.accept();
                 PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
            ) {
                while (true) {
                    String event = eventQueue.take();
                    System.out.println(String.format(event));
                    out.println(event);
                }
            } catch (IOException|InterruptedException e) {
                throw new RuntimeException("Server error", e);
            }
        }
    }
}