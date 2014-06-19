module raft.log;
import std.algorithm : min, max, reduce, sort;

public alias ulong index_t;
public alias ulong term_t;

class RaftLog
{
    LogEntry[index_t] log_by_index;
    LogEntry[string] log_by_msgid;

    this(LogEntry[index_t] log){
        if (log is null){
            auto le = new LogEntry(0, "", null);
            le.index = 0;
            le.committed = true;
            log[0] = le;
        }
        this.log_by_index = log;
        foreach(ent; this.log_by_index.byValue){
            auto msgid = ent.msgid;
            this.log_by_msgid[msgid] = ent;
        }
    }

    auto dump(){
        return this.log_by_index;
    }

    auto get_max_index_term(){
        struct pair { index_t index; term_t term; }
        auto maxindex = this.maxindex();
        auto maxterm = this.log_by_index.get(maxindex, null).term;
        return pair(maxindex, maxterm);
    }

    auto has_uuid(string uuid){
        return uuid in this.log_by_msgid;
    }

    auto maxindex(){
        return reduce!max(this.log_by_index.keys);
    }

    auto get(index_t idx){
        return this.log_by_index.get(idx, null);
    }

    auto get_by_uuid(string uuid){
        return this.log_by_msgid.get(uuid, null);
    }

    auto get_by_index(index_t index){
        return this.get(index);
    }

    auto get_term_of(index_t idx){
        auto le = this.get(idx);
        return le.term;
    }

    void remove(index_t idx){
        auto ent = this.log_by_index[idx];
        this.log_by_index.remove(idx);
        auto msgid = ent.msgid;
        this.log_by_msgid.remove(msgid);
    }

    auto add(LogEntry logentry){
        index_t index;
        if (!logentry.index){
            // this is being appended to a leader"s log; reject if msgid is
            // known and allocate a new index for it
            if (logentry.msgid in this.log_by_msgid){
                return 0;
            }
            index = this.maxindex() + 1;
            logentry.index = index;
        }
        else{
            // this is a follower being told to put logentry in a specific spot
            index = logentry.index;
            auto mi = this.maxindex();
            if (mi + 1 != index){
                // remove everything in the log after and including the current
                // index
                index_t[] remove;
                foreach(x; this.log_by_index.byKey)
                    if (x >= index)
                        remove ~= x;
                foreach(rem; remove){
                    this.remove(rem);
                }
            }
        }
        this.log_by_index[index] = logentry;
        auto msgid = logentry.msgid;
        this.log_by_msgid[msgid] = logentry;
        return index;
    }

    void add_ack(index_t index, term_t term, string uuid){
        auto ent = this.log_by_index[index];
        //if (uuid in ent.acked){
        //    return
        ent.acked[uuid] = true;
    }

    auto num_acked(index_t index){
        auto ent = this.log_by_index[index];
        return ent.acked;
    }

    void commit(index_t index, term_t term){
        auto ent = this.log_by_index[index];
        assert(ent.term == term);
        ent.committed = true;
    }

    void force_commit(index_t index){
        // this is more dangerous; only call it from followers on orders
        // from the leader
        auto ent = this.log_by_index.get(index, null);
        if (ent is null){
            return;
        }
        ent.committed = true;
    }

    auto is_committed(index_t index, term_t term){
        auto ent = this.log_by_index[index];
        if (ent.term != term){
            return false;
        }
        index = ent.index;
        return index <= this.get_commit_index();
    }

    auto is_committed_by_uuid(string uuid){
        auto ent = this.log_by_msgid.get(uuid, null);
        if (ent is null){
            return false;
        }
        auto index = ent.index;
        return index <= this.get_commit_index();
    }

    auto logs_after_index(index_t index){
        auto last = this.maxindex();
        LogEntry[index_t] logs;
        foreach(x; index..min(last, index + 50)){
            logs[x+1] = this.log_by_index[x+1];
        }
        return logs;
    }

    auto committed_logs_after_index(index_t index){
        auto last = this.get_commit_index();
        LogEntry[index_t] logs;
        foreach (x; index..last){
            logs[x+1] = this.log_by_index[x+1];
        }
        return logs;
    }

    auto get_commit_index(){
        foreach_reverse(k; sort(this.log_by_index.keys)){
            auto v = this.log_by_index[k];
            if (v.committed){
                return v.index;
            }
        }
        return 0;
    }

    auto exists(index_t index, term_t term){
        auto logentry = this.log_by_index.get(index, null);
        return logentry && logentry.term == term;
    }

    alias Object.opCmp opCmp;
    int opCmp(RaftLog other){
        auto m = this.get_max_index_term();
        auto o = other.get_max_index_term();
        if (o.term < m.term)
          return -1;
        else if (o.term > m.term)
          return 1;
        else if (o.index < m.index)
          return -1;
        else if (o.index > m.index)
          return 1;
        else
          return 0;
    }
}

class LogEntry{
    term_t term;
    string msgid;
    ubyte[] msg;
    bool committed;
    bool[string] acked;
    index_t index;
    this(term_t term, string msgid, ubyte[] msg) { this.term = term; this.msgid = msgid; this.msg = msg; }
}
