module set;

public struct set(T)
{
	shared uint id;
	uint[T] aa;

	public this(U)(U range)
	{
		foreach(k; range)
			this.add(k);
	}

	public void add(T obj) { aa[obj] = ++id; }
	public void remove(T obj) { aa.remove(obj); }

	public @property auto dup() const { return set!T(id,aa.dup); }

	public auto union_(U)(in U range)
	{
		auto ret = this.dup;
		foreach(k; range)
			ret.add(k);
		return ret;
	}

	public alias aa this;
}

unittest
{
	set!int a;
	assert(2 !in a);
	a.add(2);
	assert(2 in a);
	a.remove(2);
	assert(2 !in a);

	auto c = a.union_(a);
	auto d = set!string(["a","b"]);
	assert("a" in d);
	assert("b" in d);
	assert("c" !in d);
}
