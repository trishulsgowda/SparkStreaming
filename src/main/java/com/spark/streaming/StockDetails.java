package com.spark.streaming;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

public class StockDetails implements Serializable{

	@Override
	public String toString() {
		return "StockDetails [symbol=" + symbol + ", timestamp=" + timestamp + ", priceData=" + priceData + "]";
	}
	private String symbol;
	private String timestamp;
	private PriceData priceData;
	
	public String getSymbol() {
		return symbol;
	}
	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}
	public String getTimestamp() {
		return timestamp;
	}
	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}
	public PriceData getPriceData() {
		return priceData;
	}
	public void setPriceData(PriceData priceData) {
		this.priceData = priceData;
	}
}
