module raft.client;

import msgpack;
import std.uuid;
import tcp = raft.tcp;

public class NoConnection : Exception
{
	this() { super("NoConnection"); }
}

class RaftClient
{
    tcp.TCP tcp;

    this(string server)
	{
        this.tcp = tcp.TCP(0, "client");
        this.tcp.start();
        this.msgs = {};
        this.tcp.connect(server);
        if (!this.tcp.u2c)
            // wait 2 seconds to connect
            this.tcp.recv(0.5);
        if (!this.tcp.u2c)
            throw new NoConnection;
        this.leader = this.tcp.u2c.keys[0];
    }

    auto _send(string[string] rpc, string msgid)
    {
        this.tcp.send(rpc, this.leader);
        auto msgids = this.poll(0.5);
        if (!msgids || msgid !in msgids)
            return;  // XXX put real recovery logic here
        auto msg = this.msgs[msgid][0];
        if (msg["type"] == "cr_rdr")
        {
            this.leader = msg["leader"];
            writefln("redirected to %s! %s", this.leader, msg["addr"]);
            this.tcp.connect(msg["addr"]);
            this.msgs.remove(msgid);
            return this._send(rpc, msgid);
        }
    }

    auto poll(Duration timeout=0)
    {
        auto ans = this.tcp.recv(timeout);
        if (!ans)
            return;
        auto msgids = set();
        foreach(msgs; ans)
		{
            foreach(msg; msgs)
			{
                msg = msgpack.unpack(msg);//, use_list=False)
                msgid = msg["id"];
                msgids.add(msgid);
                ums = this.msgs.get(msgid, []);
                ums.append(msg);
                this.msgs[msgid] = ums;
			}
		}
        return msgids;
	}

    auto send(string data)
    {
        auto msgid = uuid.uuid4().hex;
        rpc = this.cq_rpc(data, msgid);
        this._send(rpc, msgid);
        return msgid;
    }

    auto update_hosts(string config)
    {
        auto msgid = randomUUID().toString();
        auto rpc = this.pu_rpc(config, msgid);
        this._send(rpc, msgid);
        return msgid;
    }

    auto cq_rpc(string data, string msgid)
	{
        // client query rpc
        auto rpc = [
            "type": "cq",
            "id": msgid,
            "data": data
        ];
        return msgpack.pack(rpc);
    }

    auto pu_rpc(string config, string msgid)
    {
        // protocol update rpc
        auto rpc = [
            "type": "pu",
            "id": msgid,
            "config": config
        ];
        return msgpack.pack(rpc);
    }
}

