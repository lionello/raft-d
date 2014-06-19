module raft.udp;

import std.datetime;
import std.socket;

public UDP start(ushort port)
{
    UDP udp = new UDP(port);
    udp.start();
    return udp;
}

class UDP
{
    uint maxmsgsize;
    ushort port;
    UdpSocket sock;
    
    this(ushort port)
    {
        this.port = port;
        this.maxmsgsize = 8192;
    }

    void start()
    {
        this.sock = new UdpSocket(AddressFamily.INET);
        this.sock.bind(new InternetAddress("", this.port));
    }

    public ubyte[] recv(Duration timeout)
    {
        SocketSet ss = new SocketSet();
        ss.add(this.sock);
        if (Socket.select(ss, null, null, timeout))
        {
            ubyte[] buf = void;
            buf.length = 65535; // max udp size
            auto ret = this.sock.receiveFrom(buf);
            if (ret > 0)
              return buf[0..ret];
        }
        return null;
    }

    public void send(in void[] msg, Address dst)
    {
        this.sock.sendTo(msg, dst);
        /*except socket.error as e:
            if e.errno == errno.EMSGSIZE:
                this.maxmsgsize /= 2
            if e.errno == errno.EPIPE:
                this.shutdown()
                this.start()*/
    }

    void shutdown()
    {
        this.sock.shutdown(SocketShutdown.BOTH);
        this.sock.close();
    }
}

