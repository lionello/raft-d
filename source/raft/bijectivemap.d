module raft.bijectivemap;
debug import std.stdio;

auto create_map(K,V)()
{
  return new BijectiveMap!(K,V);
}

class BijectiveMap(K, V)
{
  V[K] self;
  BijectiveMap!(V,K) _other;

  @property public auto other() { return _other; }

  this()
  {
    this._other = new BijectiveMap!(V,K)(this);
    assert(this._other._other is this);
  }

  private this(BijectiveMap!(V,K) other)
  {
    assert(other);
    this._other = other;
  }

  public V opIndexAssign(V value, K key)
  {
    if (key in self)
    {
      // we want to do a -> b but we already have a mapping
      // a -> c, which means the other dict has c -> a
      // first delete c from the other dict, and then proceed
      auto oldval = self[key];
      other.self.remove(oldval);
    }
    if (value in other.self)
    {
      // we're doing a -> b but we already have c -> b
      // which destroys the bijection.  remove the key c
      auto oldkey = other.self[value];
      self.remove(oldkey);
    }
    self[key] = value;
    other.self[value] = key;
    return value;
  }

  public V opIndex(KK)(KK key)
  {
    return self[key];
  }

  public auto opBinaryRight(string S)(K key) if (S == "in")
  {
    return key in self; 
  }
  
  public void remove(KK)(KK key)
  {
    auto val = self[key];
    self.remove(key);
    other.self.remove(val);
  }

  public @property auto byKey() { return self.byKey; }
}

unittest
{
  auto map = new BijectiveMap!(int,int);
  map[2] = 3;
  assert(2 in map);
  assert(map[2] == 3);
  assert(map.self[2] == 3);
  assert(map.other[3] == 2);
  map[2] = 4;
  assert(map[2] == 4);
  assert(map.self[2] == 4);
  assert(map.other[4] == 2);
  assert(3 !in map.other);
  assert(3 !in map.other.self);
  map.remove(2);
  assert(2 !in map);
  assert(2 !in map.self);
  assert(4 !in map.other);
  assert(4 !in map.other.self);
  debug writeln("success");
}
