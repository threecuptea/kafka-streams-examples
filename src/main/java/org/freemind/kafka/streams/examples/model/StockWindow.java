package org.freemind.kafka.streams.examples.model;

import java.util.Date;

/**
 * Created by fandev on 5/12/17.
 */
public class StockWindow {
    private String symbol;
    private Date start;

    public StockWindow(String symbol, Date start) {
        this.symbol = symbol;
        this.start = start;
    }
}
