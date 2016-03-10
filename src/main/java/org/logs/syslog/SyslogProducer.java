package org.logs.syslog;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class SyslogProducer {

    public void produceMultiConnection(String host, int port, int numLogs, long delayMillis, long delayEvery, int msgSize) {
        byte[] message = createMessage(msgSize);
        final ByteBuffer buf = ByteBuffer.allocate(msgSize + 100);

        final InetSocketAddress address = new InetSocketAddress(host, port);

        for (int i=0; i < numLogs; i++) {
            try (SocketChannel channel = SocketChannel.open()) {
                channel.connect(address);

                buf.clear();
                buf.put(message);
                buf.put((byte)'\n');
                buf.flip();

                while(buf.hasRemaining()) {
                    channel.write(buf);
                }

                if (delayMillis > 0 && i % delayEvery == 0) {
                    try {
                        Thread.sleep(delayMillis);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                if (i % 10000 == 0) {
                    System.out.println("Sent " + i);
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("DONE!");
    }


    public void produceTcpSingleConnection(String host, int port, int numLogs, long delayMillis, long delayEvery, int msgSize) {
        byte[] message = createMessage(msgSize);
        final ByteBuffer buf = ByteBuffer.allocate(msgSize + 100);

        final InetSocketAddress address = new InetSocketAddress(host, port);

        try (SocketChannel channel = SocketChannel.open()) {
            channel.connect(address);
            for (int i=0; i < numLogs; i++) {
                buf.clear();
                buf.put(message);
                buf.put((byte)'\n');
                buf.flip();

                while(buf.hasRemaining()) {
                    channel.write(buf);
                }

                if (delayMillis > 0 && i % delayEvery == 0) {
                    try {
                        Thread.sleep(delayMillis);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                if (i % 10000 == 0) {
                    System.out.println("Sent " + i);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("DONE!");
    }

    public void produceUdp(String host, int port, int numLogs, long delayMillis, long delayEvery, int msgSize) {
        byte[] message = createMessage(msgSize);
        final ByteBuffer buf = ByteBuffer.allocate(msgSize + 100);

        final InetSocketAddress address = new InetSocketAddress(host, port);

        try (DatagramChannel channel = DatagramChannel.open()) {
            for (int i=0; i < numLogs; i++) {
                buf.clear();
                buf.put(message);
                buf.flip();
                channel.send(buf, address);

                if (delayMillis > 0 && i % delayEvery == 0) {
                    try {
                        Thread.sleep(delayMillis);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                if (i % 10000 == 0) {
                    System.out.println("Sent " + i);
                }
            }
            System.out.println("DONE!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private byte[] createMessage(int numBytes) {
        StringBuilder builder = new StringBuilder(numBytes);
        builder.append("<34>Oct 13 22:14:15 localhost ");

        while (builder.length() < numBytes) {
            builder.append("a");
        }

        final String message = builder.toString();
        return message.getBytes(StandardCharsets.UTF_8);
    }

    public static void main(String[] args) {
        if (args == null || args.length !=7) {
            System.err.println("USAGE: SyslogProducer <tcp-single|tcp-multi|udp> host <port> <num logs> <delay millis> <delay every> <msg_size>");
            System.exit(1);
        }

        final String type = args[0];
        final String host = args[1];
        final Integer port = Integer.parseInt(args[2]);
        final Integer numLogs = Integer.parseInt(args[3]);
        final Long delayMillis = Long.parseLong(args[4]);
        final Long delayEvery = Long.parseLong(args[5]);
        final Integer msgSize = Integer.parseInt(args[6]);

        final long startTime = System.currentTimeMillis();

        final SyslogProducer producer = new SyslogProducer();
        if ("tcp-multi".equals(type)) {
            producer.produceMultiConnection(host, port, numLogs, delayMillis, delayEvery, msgSize);
        } else if ("tcp-single".equals(type)) {
            producer.produceTcpSingleConnection(host, port, numLogs, delayMillis, delayEvery, msgSize);
        } else {
            producer.produceUdp(host, port, numLogs, delayMillis, delayEvery, msgSize);
        }

        System.out.println("Completed in " + ((System.currentTimeMillis() - startTime)/1000) + " seconds");
    }

}
