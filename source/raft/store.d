module raft.store;

import std.uuid;
import msgpack;
import std.file;
import std.conv;
import errno = core.stdc.errno;
debug import std.stdio : writeln;
import raft.log;

struct State
{
  term_t term;
  ulong voted;
  LogEntry[index_t] log;
  string[string] peers;
  string uuid;
}

public State read_state(int port)
{
  auto sfile = "/tmp/raft-state-" ~ to!string(port);
  try
  {
      State state;
      msgpack.unpack(cast(ubyte[])read(sfile), state);
      return state;
  }
  catch(FileException e)
  {
      if (e.errno != errno.ENOENT)
          throw e;
  }
  // no state file exists; initialize with fresh values
  return State(0, null, null, null, randomUUID().toString());
}

void write_state(int port, State s)
{
  auto sfile = "/tmp/raft-state-" ~ to!string(port);
  write(sfile, msgpack.pack(s));
}

unittest
{
  auto s1 = read_state(0);
  assert(s1.uuid !is null);
  write_state(0, s1);
  auto s2 = read_state(0);
  assert(s1.uuid == s2.uuid);
  debug writeln("success");
}

