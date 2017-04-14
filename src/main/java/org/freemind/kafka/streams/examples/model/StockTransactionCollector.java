package org.freemind.kafka.streams.examples.model;

/**
 * Created by fandev on 4/11/17.
 */
public class StockTransactionCollector {

    private double amount;
    private String tickerSymbol;
    private double sharesPurchased;
    private double sharesSold;


    public StockTransactionCollector add(StockTransaction transaction) {
        if (tickerSymbol == null) {
            tickerSymbol = transaction.getSymbol();
        }
        this.amount += transaction.getAmount();
        if (transaction.getType().equals("purchase")) {
            this.sharesPurchased += transaction.getShares();
        } else {
            this.sharesSold += transaction.getShares();
        }
        return this;
    }

    @Override
    public String toString() {
        return "StockTransactionCollector{" +
                "amount=" + amount +
                ", tickerSymbol='" + tickerSymbol + '\'' +
                ", sharesPurchased=" + sharesPurchased +
                ", sharesSold=" + sharesSold +
                '}';


    }
}