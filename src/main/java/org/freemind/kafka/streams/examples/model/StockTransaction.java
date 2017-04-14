package org.freemind.kafka.streams.examples.model;

import java.util.Date;

/**
 * Created by fandev on 4/11/17.
 */
public class StockTransaction {

    private String symbol;
    private String type;
    private double shares;
    private double amount;
    private Date timeStamp;

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public double getShares() {
        return shares;
    }

    public void setShares(double shares) {
        this.shares = shares;
    }

    public double getAmount() {
        return amount;
    }

    public void setAmount(double amount) {
        this.amount = amount;
    }

    public Date getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Date timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "StockTransaction{" +
                "symbol='" + symbol + '\'' +
                ", type='" + type + '\'' +
                ", shares=" + shares +
                ", amount=" + amount +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
