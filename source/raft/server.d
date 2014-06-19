module raft.server;

import msgpack;

import std.socket;
import store = raft.store;
import channel = raft.tcp;
import log = raft.log;
import threading = core.thread;
import set;

alias string[string] msg_t;

class Queue
{
	void put(msg_t, string){}
}

auto make_server(ushort port=9289, Address[] bootstraps=null){
    auto queue = Queue();
    server = new Server(queue, port, bootstraps);
    server.start();
    return queue;
}

class Server : threading.Thread
{
    immutable ushort port;
    Queue queue;
    string role;
    channel.TCP channel;
    index_t commitidx;
    string update_uuid;
    bool daemon;
    set!string cronies;
    set!string refused;
    string uuid;
    Address[] bootstraps;
    RaftLog log;
	Address[string] peers;
    
    this(Queue queue, ushort port, Address[] bootstraps){
        this.port = port;
        this.load();
        this.bootstraps = bootstraps;
        this.queue = queue;
        this.role = "follower";
        this.channel = channel.start(port, this.uuid);
        this.last_update = time.time();
        this.commitidx = 0;
        this.update_uuid = null;
        this.leader = null;
        this.newpeers = null;
        this.oldpeers = null;
        this.daemon = true;

		InitDispatchTable();
    }

    //
    // startup && state methods
    //

    auto load(){
        auto state = store.read_state(this.port);
        this.term = state.term;
        this.voted = state.voted;
        this.peers = state.peers;
        this.uuid = state.uuid;
        this.log = log.RaftLog(state.log);
    }

    auto save(){
        store.write_state(this.port, State(this.term, this.voted,
                          this.log.dump(), this.peers, this.uuid));
    }

    auto run(){
        this.running = true;
        while (this.running){
            foreach(peer; this.peers)
                if (peer !in this.channel && peer != this.uuid)
                    this.channel.connect(this.peers[peer]);
            foreach(addr; this.bootstraps)
                this.channel.connectbs(addr, this.bootstrap_cb);
            channelans = this.channel.recv(0.15);
            if (channelans){
                foreach(peer, msgs; channelans)
                    foreach(msg; msgs)
                        this.handle_message(msg, peer);
            }
            else
                this.housekeeping();
		}
    }

    //
    // message handling
    //

    auto handle_message(msg_t msg, string addr){
        // got a new message
        // update our term if applicable, && dispatch the message
        // to the appropriate handler.  finally, if we are still
        // (or have become) the leader, send out heartbeats
        ubyte[] msg;
        try{
            msg = msgpack.unpack(msg, use_list=false);
        }
        catch (msgpack.UnpackException){
            return;
        }
        mtype = msg["type"];
        term = msg.get("term", null);
        msg["src"] = addr;
        uuid = msg.get("id", null);
        // no matter what, if our term is old, update && step down
        if (term && term > this.term && this.valid_peer(uuid)){
            // okay, well, only if it's from a valid source
            this.term = term;
            this.voted = null;
            this.role = "follower";
		}
        auto mname = "handle_msg_"~this.role~"_"~mtype;
		dispatchTable.get(mname, &handle_msg_unknown)(msg);
        if (this.role == "leader" && time.time() - this.last_update > 0.3){
            this.send_ae();
        }
    }

	private alias void delegate(msg_t) MsgHandler_t;
	private MsgHandler_t[string] dispatchTable;
	private void InitDispatchTable()
	{
		foreach (memberName; __traits(allMembers, Server))
			if (memberName[0..10] == "handle_msg_")
				dispatchTable[memberName] = &__traits(getMember, Server, memberName);
	}

	void handle_msg_unknown(msg_t msg){ assert(0); }

    void handle_msg_candidate_bootstrap(msg_t msg){
        this.handle_msg_follower_bootstrap(msg);
    }

    void handle_msg_follower_bootstrap(msg_t msg){
        // bootstrap packets solve the problem of how we find the
        // id of our peers.  we don't want to have to copy uuids around
        // when they could just mail them to each other.
        writeln(msg);
        writeln(this.peers);
    }

    void handle_msg_leader_ae_reply(msg_t msg){
        // we are a leader who has received an ae ack
        // if the update was rejected, it's because the follower
        // has an incorrect log entry, so send an update for that
        // log entry as well
        // if the update succeeded, record that in the log and,
        // if the log has been recorded by enough followers, mark
        // it committed.
        auto uuid = msg["id"];
        if (!this.valid_peer(uuid)){
            return;
        }
        auto success = msg["success"];
        auto index = msg["index"];
        if (success){
            this.next_index[uuid] = index;
            if (this.log.get_commit_index() < index){
                this.msg_recorded(msg);
			}
        }
        else{
            // exponentially reduce the index for peers
            // this way if they're only missing a couple log entries,
            // we only have to send 2 || 4, but if they're missing
            // a couple thousand we'll find out in less than 2k round
            // trips
            oldidx = this.next_index.get(uuid, 0);
            diff = this.log.maxindex() - oldidx;
            diff = max(diff, 1);
            oldidx -= diff;
            this.next_index[uuid] = max(oldidx, 0);
       }
    }
	
    void handle_msg_follower_ae(msg_t msg){
        // we are a follower who just got an append entries rpc
        // reset the timeout counter
        auto uuid = msg["id"];
        if (!this.valid_peer(uuid)){
            return;
        }
        auto term = msg["term"];
        if (term < this.term){
            return;
        }
        this.last_update = time.time();
        this.leader = msg["id"];
        auto logs = msg["entries"];
        auto previdx = msg["previdx"];
        auto prevterm = msg["prevterm"];
        if (!this.log.exists(previdx, prevterm)){
            rpc = this.ae_rpc_reply(previdx, prevterm, false);
            this.send_to_peer(rpc, this.leader);
            return;
        }
        auto cidx = msg["commitidx"];
        if (cidx > this.commitidx){  // don't lower the commit index
            this.commitidx = cidx;
            this.log.force_commit(cidx);
            if (this.update_uuid){
                this.check_update_committed();
            }
        }
        if (!logs){
            // heartbeat
            return;
        }
        foreach (ent; sorted(logs)){
            auto val = logs[ent];
            this.process_possible_update(val);
            this.log.add(val);
        }
        auto maxmsg = this.log.get_by_index(this.log.maxindex());
        auto rpc = this.ae_rpc_reply(maxmsg["index"], maxmsg["term"], true);
        this.send_to_peer(rpc, this.leader);
    }
	
    void handle_msg_candidate_ae(msg_t msg){
        // someone else was elected during our candidacy
        term = msg["term"];
        uuid = msg["id"];
        if (!this.valid_peer(uuid)){
            return;
        }
        if (term < this.term){
            // illegitimate, toss it
            return;
        }
        this.role = "follower";
        this.handle_msg_follower_ae(msg);
    }

    void handle_msg_follower_cq(msg_t msg){
        try{
            auto rpc = this.cr_rdr_rpc(msg["id"]);
            src = msg["src"];
            this.send_to_peer(rpc, src);
        }catch{
            // we're allowed not to respond at all, in this case,
            // so if we crashed for some reason, just ignore it
        }
    }

    void handle_msg_leader_cq(msg_t msg){
        auto src = msg["src"];
        if (msg["id"] is null){
            auto msgid = uuid.uuid4().hex;
            msg["id"] = msgid;
        }
        this.add_to_log(msg);
        auto rpc = this.cr_rpc_ack(msg["id"]);
        this.send_to_peer(rpc, src);
    }

    void handle_msg_leader_cq_inq(msg_t msg){
        auto src = msg["src"];
        auto msgid = msg["id"];
        auto info = [];
        auto inquiry = this.log.get_by_uuid(msgid);
        if (inquiry is null){
            info["status"] = "unknown";
        }
        else if (inquiry["index"] > this.commitidx){
            info["status"] = "pending";
        }
        else{
            info["status"] = "committed";
        }
        auto rpc = this.cr_rpc_ack(msgid, info);
        this.send_to_peer(rpc, src);
    }

    void handle_msg_candidate_rv(msg_t msg){
        // don't vote for a different candidate!
        auto uuid = msg["id"];
        if (this.uuid == uuid){
            // huh
            return;
        }
        if (this.valid_peer(uuid)){
            return;
        }
        auto rpc = this.rv_rpc_reply(false);
        this.send_to_peer(rpc, uuid);
    }

    void handle_msg_follower_rv(msg_t msg){
        auto term = msg["term"];
        auto uuid = msg["id"];
        if (!this.valid_peer(uuid)){
            return;
        }
        auto olog = [msg["log_index"]: [
                    "index": msg["log_index"],
                    "term": msg["log_term"],
                    "msgid": "",
                    "msg": [] ]  ];
        olog = log.RaftLog(olog);
        if (term < this.term){
            // someone with a smaller term wants to get elected
            // as if
            rpc = this.rv_rpc_reply(false);
            this.send_to_peer(rpc, uuid);
            return;
        }
        if ((this.voted is null || this.voted == uuid) && this.log <= olog){
            // we can vote for this guy
            this.voted = uuid;
            this.save();
            auto rpc = this.rv_rpc_reply(true);
            this.last_update = time.time();
            this.send_to_peer(rpc, uuid);
            return;
        }
        // we probably voted for somebody else, || the log is old
        rpc = this.rv_rpc_reply(false);
        this.send_to_peer(rpc, uuid);
    }

    void handle_msg_candidate_rv_reply(msg_t msg){
        uuid = msg["id"];
        if (!this.valid_peer(uuid))
            return;
        voted = msg["voted"];
        if (voted)
            this.cronies.add(uuid);
        else
            this.refused.add(uuid);
        if (this.cronies.length >= this.quorum()){
            // won the election
            this.role = "leader";
            this.next_index = [];
            this.commitidx = this.log.get_commit_index();
            maxidx = this.log.maxindex();
            foreach (uuid; this.all_peers){
                // just start by pretending everyone is caught up,
                // they'll let us know if not
                this.next_index[uuid] = maxidx;
			}
		}
    }

    void handle_msg_leader_pu(msg_t msg){
        if (this.update_uuid){
            // we"re either already in the middle of this, || we're
            // in the middle of something *else*, so piss off
            return;
		}
        auto uuid = msg["id"];
        // got a new update request
        // it will consist of machines to add && to remove
        // here we perform the first phase of the update, by
        // telling clients to add the new machines to their
        // existing peer set.
        msg["phase"] = "1";
        this.newpeers = msg["config"]; // adopt the new config right away
        if (!this.newpeers){
            return;
		}
        this.update_uuid = uuid;
        this.add_to_log(msg);
    }

    auto housekeeping(){
        auto now = time.time();
        if (this.role == "candidate"){
            elapsed = now - this.election_start;
		}
        if (now - this.last_update > 0.5 && this.role == "follower"){
            // got no heartbeats; leader is probably dead
            // establish candidacy && run for election
            this.call_election();
		}
        else if (this.role == "candidate" && elapsed < this.election_timeout){
            // we"re in an election && haven't won, but the
            // timeout isn"t expired.  repoll peers that haven't
            // responded yet
            this.campaign();
		}
        else if (this.role == "candidate"){
            // the election timeout *has* expired, && we *still*
            // haven't won || lost.  call a new election.
            this.call_election();
		}
        else if (this.role == "leader"){
            // send a heartbeat
            this.send_ae();
        }
    }

    //
    // convenience methods
    //

    auto send_ae(){
        this.last_update = time.time();
        foreach (uuid; this.all_peers){
            if (uuid == this.uuid)  // no selfies
                continue;
            auto ni = this.next_index.get(uuid, this.log.maxindex());
            logs = this.log.logs_after_index(ni);
            auto rpc = this.ae_rpc(uuid, logs);
            this.send_to_peer(rpc, uuid);
        }
    }

    auto call_election(){
        this.term += 1;
        this.voted = this.uuid;
        this.save();
        this.cronies = null;
        this.refused = null;
        this.cronies.add(this.uuid);
        this.election_start = time.time();
        this.election_timeout = 0.5 * random.random() + 0.5;
        this.role = "candidate";
        this.campaign();
    }

    auto campaign(){
        auto voted = this.cronies.union_(this.refused);  // everyone who voted
        auto voters = set(this.peers);
        if (this.newpeers){
            voters = voters.union_(this.newpeers);
        }
        auto remaining = voters.difference(voted);  // peers who haven't
        auto rpc = this.rv_rpc();
        foreach(uuid; remaining)
            this.send_to_peer(rpc, uuid);
    }

    auto check_update_committed(){
        // we (a follower) just learned that one || more
        // logs were committed, *and* we are in the middle of an
        // update.  check to see if that was phase 2 of the update,
        // && remove old hosts if so
        umsg = this.log.get_by_uuid(this.update_uuid);
        if (umsg["index"] > this.commitidx){
            // isn't yet committed
            return;
        }
        data = umsg["msg"];
        if (data["phase"] == 2){
            this.oldpeers = null;
            this.update_uuid = null;
            if (this.uuid !in this.all_peers){
                this.running = false;
            }
        }
    }

    auto process_possible_update(msg_t msg){
        if (!"msg" in msg){
            return;
        }
        data = msg["msg"];
        if (!"type" in data){
            return;
        }
        if (data["type"] != "pu"){
            return;
        }
        phase = data["phase"];
        uuid = data["id"];
        if (this.update_uuid == uuid){
            // we've already done this
            return;
        }
        this.update_uuid = uuid;  // in case we become leader during this debacle
        if (phase == 1){
            this.newpeers = data["config"];
        }
        else if (phase == 2 && this.newpeers){
            this.oldpeers = this.peers;
            this.peers = this.newpeers;
            this.newpeers = null;
        }
    }

    auto possible_update_commit(){
        // we're in an update; see if the update msg
        // has committed, && go to phase 2 || finish
        if (!this.log.is_committed_by_uuid(this.update_uuid)){
            // it hasn't
            return;
        }
        umsg = this.log.get_by_uuid(this.update_uuid);
        data = copy.deepcopy(umsg["msg"]);
        if (data["phase"] == 1 && this.newpeers){
            // the *first* phase of the update has been committed
            // new leaders are guaranteed to be in the union of the
            // old && new configs.  now update the configuration
            // to the new one only.
            data["phase"] = 2;
            newid = uuid.uuid4().hex;
            this.update_uuid = newid;
            data["id"] = newid;
            this.oldpeers = this.peers;
            this.peers = this.newpeers;
            this.newpeers = null;
            logentry = log.logentry(this.term, newid, data);
            this.log.add(logentry);
        }
        else{
            // the *second* phase is now committed.  tell all our
            // current peers about the successful commit, drop
            // the old config entirely and, if necessary, step down
            this.send_ae();  // send this to peers who might be about to dispeer
            this.oldpeers = null;
            this.update_uuid = null;
            if (!this.uuid in this.peers){
                this.running = false;
            }
        }
    }

    auto all_peers(int delegate(ref string) dg){
        int result;
        foreach (host; this.peers){
            result = dg(host);
            if (result)
                break;
        }
        if (this.newpeers)
            foreach (host; this.newpeers){
                result = dg(host);
                if (result)
                    break;
            }
        if (this.oldpeers)
            foreach (host; this.oldpeers){
                result = dg(host);
                if (result)
                    break;
            }
        return result;
    }

    auto valid_peer(uuid){
        if (uuid in this.peers){
            return true;
		}
        if (this.newpeers && uuid in this.newpeers){
            return true;
		}
        if (this.oldpeers && uuid in this.oldpeers){
            return true;
		}
        return false;
    }
		
    auto get_peer_addr(uuid){
        if (uuid in this.peers){
            return this.peers[uuid];
		}
        if (this.newpeers && uuid in this.newpeers){
            return this.newpeers[uuid];
		}
        if (this.oldpeers && uuid in this.oldpeers){
            return this.oldpeers[uuid];
		}
    }

    auto send_to_peer(rpc, uuid){
        this.channel.send(rpc, uuid);
    }

    auto quorum(){
        auto peers = set(this.peers);
        if (this.newpeers){
            peers = peers.union_(this.newpeers);
        }
        // oldpeers don't get a vote
        // use sets because there could be dupes
        auto np = peers.length;
        return np/2 + 1;
    }

    auto msg_recorded(msg_t msg){
        // we're a leader && we just got an ack from
        // a follower who might have been the one to
        // commit an entry
        term = msg["term"];
        index = msg["index"];
        uuid = msg["id"];
        this.log.add_ack(index, term, uuid);
        if (this.log.num_acked(index) >= this.quorum() && term == this.term){
            this.log.commit(index, term);
            assert(index >= this.commitidx);
            oldidx = this.commitidx;
            this.commitidx = index;
            if (this.update_uuid){
                // if there's an update going on, see if our commit
                // is actionable
                this.possible_update_commit();
            }
            // otherwise just see what messages are now runnable
            this.run_committed_messages(oldidx);
		}
    }
	
    auto add_to_log(msg_t msg){
        auto uuid = msg["id"];
        auto logentry = log.logentry(this.term, uuid, msg);
        auto index = this.log.add(logentry);
        this.save();
        this.log.add_ack(index, this.term, this.uuid);
    }

    auto run_committed_messages(oldidx){
        auto committed = this.log.committed_logs_after_index(oldidx);
        foreach(_, val; sorted(committed)){
            msg = val["msg"];
            msgid = msg["id"];
            data = msg["data"];
            this.queue.put(msgid, data);
		}
    }

    auto bootstrap_cb(uuid, addr){
        this.bootstraps.remove(addr);
        this.peers[uuid] = addr;
    }

    //
    // rpc methods
    //

    auto rv_rpc(){
        log_index, log_term = this.log.get_max_index_term();
        auto rpc = [
            "type": "rv",
            "term": this.term,
            "id": this.uuid,
            "log_index": log_index,
            "log_term": log_term,
        ];
        return msgpack.pack(rpc);
    }

    auto rv_rpc_reply(voted){
        auto rpc = [
            "type": "rv_reply",
            "id": this.uuid,
            "term": this.term,
            "voted": voted
        ];
        return msgpack.pack(rpc);
    }
	
    auto ae_rpc(peeruuid, append={}){
        auto previdx = this.next_index.get(peeruuid, this.log.maxindex());
        auto rpc = [
            "type": "ae",
            "term": this.term,
            "id": this.uuid,
            "previdx": previdx,
            "prevterm": this.log.get_term_of(previdx),
            "entries": append,
            "commitidx": this.commitidx,
        ];
        return msgpack.pack(rpc);
    }
	
    auto ae_rpc_reply(string index, string term, string success){
        auto rpc = [
            "type": "ae_reply",
            "term": term,
            "id": this.uuid,
            "index": index,
            "success": success
        ];
        return msgpack.pack(rpc);
    }

    auto cr_rpc(string qid, string ans){
        // client response RPC
        // qid = query id, ans is arbitrary data
        // if the qid is null, we make one up and
        // return it when we ack it
        auto rpc = [
            "type": "cr",
            "id": qid,
            "data": ans
        ];
        return msgpack.pack(rpc);
    }

    auto cr_rpc_ack(string qid, string info=null){
        // client response RPC
        // qid = query id, ans is arbitrary data
        auto rpc = [
            "type": "cr_ack",
            "id": qid,
            "info": info
        ];
        return msgpack.pack(rpc);
    }

    auto cr_rdr_rpc(msgid){
        // client response redirect; just point them
        // at the master
        if (!this.leader){
            // we don't know where to send them
            throw new RuntimeError();
        }
        auto rpc = [
            "type": "cr_rdr",
            "id": msgid,
            "addr": this.get_peer_addr(this.leader),
            "leader": this.leader
        ];
        return msgpack.pack(rpc);
    }

    auto bootstrap_rpc(){
        auto rpc = [
            "type": "bootstrap",
            "id": this.uuid
        ];
        return msgpack.pack(rpc);
    }
}