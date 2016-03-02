# syslog-producer

Utility to generate syslog messages over UDP or TCP.

## Usage

    SyslogProducer <tcp-single|tcp-multi|udp> <port> <num logs> <delay millis> <delay every> <msg size>

* tcp-single sends all messages over a single tcp connection, messages delimited by new lines
* tcp-multi sends each message over a new connection
* udp sends messages over udp
* port is the port to send to
* num logs is the number of messages to send
* delay millis is how much to sleep between messages, set to 0 for no sleeping
* delay every is how often to sleep for the amount of time specified in delay millis
* msg size is the size of each message to send in bytes (i.e. 1024, 2048, etc)

Example - Send a million messages over UDP to port 7087, sleeping for 1ms every 20 messages with 1KB messages:

    java -jar target/syslog-producer-1.0-SNAPSHOT.jar udp 7087 1000000 1 20 1024


