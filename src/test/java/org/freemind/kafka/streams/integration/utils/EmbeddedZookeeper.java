package org.freemind.kafka.streams.integration.utils;

import org.apache.zookeeper.server.NIOServerCnxnFactory;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;

/**
 * Try to get embedded kafka server 0.10.2.0 work for integration test
 *
 * This comes from https://gist.github.com/vmarcinko/e4e58910bcb77dac16e9
 */

public class EmbeddedZookeeper {
    private int port = -1;
    private int tickTime = 500;

    private ServerCnxnFactory factory;
    private File snapshotDir;
    private File logDir;

    public EmbeddedZookeeper() {
        this(-1);
    }

    public EmbeddedZookeeper(int port) {
        this(port, 500);
    }

    public EmbeddedZookeeper(int port, int tickTime) {
        this.port = resolvePort(port);
        this.tickTime = tickTime;
    }

    private int resolvePort(int port) {
        if (port == -1) {
            return TestUtils.getAvailablePort();
        }
        return port;
    }

    public void startup() throws IOException{
        if (this.port == -1) {
            this.port = TestUtils.getAvailablePort();
        }
        this.factory = NIOServerCnxnFactory.createFactory(new InetSocketAddress("127.0.0.1", port), 1024);
        this.snapshotDir = TestUtils.constructTempDir("embedded-zk/snapshot");
        this.logDir = TestUtils.constructTempDir("embedded-zk/log");

        try {
            factory.startup(new ZooKeeperServer(snapshotDir, logDir, tickTime));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }


    public void shutdown() {
        factory.shutdown();
        try {
            TestUtils.deleteFile(snapshotDir);
        } catch (FileNotFoundException e) {
            // ignore
        }
        try {
            TestUtils.deleteFile(logDir);
        } catch (FileNotFoundException e) {
            // ignore
        }
    }

    public String connectString() {
        return getConnection();
    }

    public String getConnection() {
        return"127.0.0.1:" + port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setTickTime(int tickTime) {
        this.tickTime = tickTime;
    }

    public int getPort() {
        return port;
    }

    public int getTickTime() {
        return tickTime;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("EmbeddedZookeeper{");
        sb.append("connection=").append(getConnection());
        sb.append('}');
        return sb.toString();
    }
}
