The in memory data store we use is always a skip list. The reason for using a skiplist is that we have O(Log(n)) search time,
but we also get a simple balanced structure which reduces comlexity and having to rebalance etc.

The structure of a node looks like

# Node
------
int32 - Key length
------
Key data
------
Node State - byte
------
Node height - byte
------
List of long pointers * Node Height (each long)
------
Pointer to data (Serialized Memory<byte>)

The skiplist just allocates a list of buffers all the same size. A node cannot span two buffers so it will just leave a "hole" if there is
space at the end of the buffer. This is a speed/complexity vs memory tradeoff.

The actual data isn't stored in the node, this is to get the nodes as close together as possible because you are more likely to be scanning
through a number of nodes before you return the actual data.

We can use the Memory<byte> as a "pointer" to the data as this isn't stored to disk but just an inmemory format. This allows us to take some shortcuts

When we remove a node we just update the node state, this is to allow us to "know" a delete recently happened and not go searching for the 
row in disk files only to find out its deleted. This will also be stored in the on disk file. We should clean out the data at this point.

