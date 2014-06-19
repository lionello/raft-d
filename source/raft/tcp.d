module raft.tcp;

import std.socket;
import std.uuid;
import raft.bijectivemap;
import errno = core.stdc.errno;
import core.time : Duration;
import core.thread;
import core.bitop : bswap;
import threading = core.thread;

public TCP start(ushort port, string uuid)
{
    auto tcp = new TCP(port, uuid);
    tcp.start();
    return tcp;
}

public class TCP
{
    enum greeting = "howdy!";
    immutable ushort port;
    immutable string uuid;
    bool[Socket] unknowns;//set
    TcpSocket srv;
    shared bool running;
    ubyte[][Socket] data;

    BijectiveMap!(Address,Socket) a2c;
    @property auto c2a() { return a2c.other; }
    BijectiveMap!(Socket,string) c2u;
    @property auto u2c() { return c2u.other; }

    this(ushort port, string uuid)
    {
        this.port = port;
        //this.connections = {}
        this.c2u = create_map!(Socket, string)();
        //this.data = {};
        //this.unknowns = set();
        this.a2c = create_map!(Address, Socket)();
        this.uuid = uuid;
    }

    public auto opBinaryRight(string S)(string uuid) if (S == "in")
    {
        return uuid in this.u2c;
    }

    void start()
    {
        this.running = true;
        this.srv = new TcpSocket(AddressFamily.INET);
        this.srv.setOption(SocketOptionLevel.SOCKET, SocketOption.REUSEADDR, 1);
        this.srv.bind(new InternetAddress("", this.port));
        auto thread = new threading.Thread(&this.accept);
        thread.start();
    }

    bool connect(Address addr)
    {
        if (addr in this.a2c)
            return false;
        auto conn = new TcpSocket(AddressFamily.INET);
        try{
            conn.connect(addr);
        }
        catch(SocketOSException e){
            if (e.errorCode == errno.ECONNREFUSED)
                return false;
            throw e;
        }
        conn.blocking = false;
        this.a2c[addr] = conn;
        this.add_unknown(conn);
        return true;
    }

    void accept()
    {
        this.srv.listen(5);
        while (this.running)
        {
            try{
                auto conn = this.srv.accept();
                this.a2c[conn.remoteAddress] = conn;
                conn.blocking = false;
                this.add_unknown(conn);
            }
            catch (SocketOSException e){
                if (e.errorCode == errno.ECONNABORTED)
                    continue;
            }
        }
    }

    static SocketSet toSocketSet(R)(R range)
    {
        auto ss = new SocketSet();
        foreach(c; range)
            ss.add(c);
        return ss;
    }

    auto recv(Duration timeout = Duration.zero)
    {
        auto ss = toSocketSet(this.c2u.byKey);
        try{
            Socket.select(ss, null, null, timeout);
        }
        catch(SocketOSException e){
            if (e.errorCode == errno.EINTR)
                return null;
            throw e;
        }
        struct pair { string uuid; ubyte[][] msgs; }
        pair[] rcvd;
        foreach (conn; c2u.byKey)
        {
            if (!ss.isSet(conn)) continue;
            auto msgs = this.read_conn_msg(conn);
            if (msgs)
            {
                auto uuid = this.c2u[conn];
                rcvd ~= pair(uuid, msgs);
            }
        }
        this.read_unknowns();
        return rcvd;
    }

    void add_unknown(Socket conn)
    {
        this.unknowns[conn] = true;
        auto msgsize = pack(this.greeting.length +
                            this.uuid.length + uint.sizeof);
        try{
            auto sent = 0;
            auto msg = msgsize ~ cast(ubyte[])this.greeting ~ cast(ubyte[])this.uuid;
            while (sent < msg.length)
                sent += conn.send(msg[sent..$]);//, socket.MSG_DONTWAIT);
        }
        catch(SocketOSException){
            this.unknowns.remove(conn);
            return;
        }
        this.read_unknowns();
    }

    void read_unknowns()
    {
        auto ss = new SocketSet();
        foreach (c; this.unknowns.byKey)
            ss.add(c);
        Socket.select(ss, null, null, 0);
        foreach (conn; this.unknowns.byKey)
        {
            if (!ss.isSet(conn)) continue;
            auto uuids = this.read_conn_msg(conn, 1);
            if (uuids)
            {
                assert(uuids.length == 0);
                auto uuid = cast(char[])uuids[0].idup;
                assert(uuid[0..this.greeting.length] == this.greeting[]);
                uuid = uuid[this.greeting.length..$];
                this.u2c[uuid.dup] = conn;
                this.unknowns.remove(conn);
            }
        }
    }

    auto read_conn_msg(Socket conn, size_t msgnum=0)
    {
        ubyte[] data = void;
        try{
            data.length = 4092;
            auto r = conn.receive(data);
            data = data[0..r];
        }
        catch(SocketOSException){
            this.remconn(conn);
            return null;
        }
        if (data.length == 0)
        {
            this.remconn(conn);
            this.c2u.remove(conn);
            this.data.remove(conn);
            return null;
        }
        auto buff = this.data.get(conn, null);
        buff ~= data;
        this.data[conn] = buff;
        ubyte[][] msgs;
        foreach (count, msg; this.extract_msg(conn))
        {
            msgs ~= msg;
            if (msgnum && count >= msgnum)
                return msgs;
        }
        return msgs;
    }

  auto extract_msg(Socket conn)
  {
    // return the delegate for iterating over the given connection
    return delegate int (int delegate(ref uint, ref ubyte[]) dg)
    {
      auto buff = this.data[conn];
      auto isize = uint.sizeof;
      if (buff.length < isize){
        // can't even get the length of the next message
        return 1;
      }
      uint no=0;
      int result = 0;
      while (buff.length > isize)
      {
        auto size = unpack(buff[0..isize]);
        if (buff.length < size)
          break;
        auto msg = buff[isize..size];
        buff = buff[size..$];
        this.data[conn] = buff;
        //yield msg;
        result = dg(no, msg);
        if (result)
          break;
        ++no;
      }   
      return result;
    };
  }

  alias hton = core.bitop.bswap;
  alias ntoh = core.bitop.bswap;

  static uint unpack(ubyte[] size) pure nothrow
  {
    auto n = cast(uint[])size[0..uint.sizeof];
    return ntoh(n[0]);
  }

  static ubyte[] pack(size_t size) pure nothrow
  {
    auto n = hton(cast(uint)size);
    return cast(ubyte[])((&n)[0..1]);
  }

  void send(ubyte[] msg, string uuid)
  {
      auto msgsize = pack(msg.length + uint.sizeof);
      Socket conn;
      if (uuid !in this.u2c)
          return;
      conn = this.u2c[uuid];
      try{
          auto sent = 0;
          msg = msgsize ~ msg;
          while (sent < msg.length)
          {
              sent += conn.send(msg[sent..$]);//, socket.MSG_DONTWAIT);
          }
      }
      catch(SocketOSException e){
          if (e.errorCode != errno.EPIPE)
          {
              auto addr = conn.localAddress;
              this.connect(addr);
          }
      }
  }

  void remconn(Socket conn)
  {
      if (conn in this.c2u)
          this.c2u.remove(conn);
      if (conn in this.c2a)
          this.c2a.remove(conn);
      if (conn in this.data)
          this.data.remove(conn);
  }

  void shutdown()
  {
      this.running = false;
      this.srv.shutdown(SocketShutdown.BOTH);
      this.srv.close();
  }
}

