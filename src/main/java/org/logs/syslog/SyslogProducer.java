package org.logs.syslog;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;

public class SyslogProducer {

    static final ByteBuffer buf = ByteBuffer.allocate(1024);
    static final byte[] MESSAGE = "<34>Oct 13 22:14:15 localhost this is the body of the message".getBytes(Charset.forName("UTF-8"));

    public void produceMultiConnection(int port, int numLogs, long delayMillis) {
        final InetSocketAddress address = new InetSocketAddress("localhost", port);

        for (int i=0; i < numLogs; i++) {
            try (SocketChannel channel = SocketChannel.open()) {
                channel.connect(address);

                buf.clear();
                buf.put(MESSAGE);
                buf.put((byte)'\n');
                buf.flip();

                while(buf.hasRemaining()) {
                    channel.write(buf);
                }

                if (delayMillis > 0) {
                    try {
                        //System.out.println("Sleeping for " + delayMillis + " millis");
                        Thread.sleep(delayMillis);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                if (i % 100 == 0) {
                    System.out.println("Sent " + i);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("DONE!");
    }

    public void produceTcpSingleConnection(int port, int numLogs, long delayMillis) {
        final InetSocketAddress address = new InetSocketAddress("localhost", port);

        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(address);
            for (int i=0; i < numLogs; i++) {
                buf.clear();
                buf.put(MESSAGE);
                buf.put((byte)'\n');
                buf.flip();

                while(buf.hasRemaining()) {
                    channel.write(buf);
                }

                if (delayMillis > 0) {
                    try {
                        //System.out.println("Sleeping for " + delayMillis + " millis");
                        Thread.sleep(delayMillis);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                if (i % 100 == 0) {
                    System.out.println("Sent " + i);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("DONE!");
    }

    public void produceUdp(int port, int numLogs, long delayMillis) {
        final InetSocketAddress address = new InetSocketAddress("localhost", port);

        try (DatagramChannel channel = DatagramChannel.open()) {
            for (int i=0; i < numLogs; i++) {
                buf.clear();
                buf.put(MESSAGE);
                buf.flip();
                channel.send(buf, address);

                if (delayMillis > 0) {
                    try {
                        //System.out.println("Sleeping for " + delayMillis + " millis");
                        Thread.sleep(delayMillis);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                if (i % 100 == 0) {
                    System.out.println("Sent " + i);
                }
            }
            System.out.println("DONE!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        if (args == null || args.length !=4) {
            System.err.println("USAGE: SyslogProducer <tcp-single|tcp-multi|udp> <port> <num logs> <delay millis>");
            System.exit(1);
        }

        final String type = args[0];
        final Integer port = Integer.parseInt(args[1]);
        final Integer numLogs = Integer.parseInt(args[2]);
        final Long delayMillis = Long.parseLong(args[3]);

        final long startTime = System.currentTimeMillis();

        final SyslogProducer producer = new SyslogProducer();
        if ("tcp-multi".equals(type)) {
            producer.produceMultiConnection(port, numLogs, delayMillis);
        } else if ("tcp-single".equals(type)) {
            producer.produceTcpSingleConnection(port, numLogs, delayMillis);
        } else {
            producer.produceUdp(port, numLogs, delayMillis);
        }

        System.out.println("Completed in " + ((System.currentTimeMillis() - startTime)/1000) + " seconds");
    }

}
