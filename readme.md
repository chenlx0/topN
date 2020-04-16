# topN

这个程序可以找到一个大文件中出现次数前 N 多的行。例如一个包含有数百万行 URL 的文件。

## 它是如何工作的

想要在一个文件中找到前 N 多的行很容易，但是当这个文件大到无法被全部加载到内存中时，就变得有点困难了。

这个程序将大文件切割成若干小文件，然后将小文件聚合以解决这个问题。

### 步骤 1: 切割文件

我在这里借鉴了 Map Reduce 的想法：

Map 任务逐行读取源文件数据，并通过 channel 发送给 Reduce 任务。

Reduce 任务接收数据，计算每行的哈希值（在这里使用 MD5 算法），并将行数据放到 Reduce 任务独占的 map 中，如果该行数据的哈希值中 map 中已有记录，那么只需要把对应的 occurs 值自增 1。当 map 的数据量足够大时，会被写入到小的二进制文件中。配置文件 conf.json 中指定了小文件的数量 splitNum，Reduce 任务根据行数据的哈希值决定其被保存到哪个文件中。对于哈希值为 K 的数据，会被保存到第 (K mod splitNum) 个文件中。因此，相同的行会被保存到相同的小文件中。

另外，哈希函数具有均匀性，因此我们认为所有行将会被均匀分布到小文件中。由于我们在 Reduce 任务中已经用 map 做了一定程度的聚合，因此不需要担心源文件中有大量重复的行，使得我们的某几个小文件特别大。

每一行会被以如下格式保存：

```
+-----------------+---------------+----------------+
| HASH VALUE(MD5) | OFFSET(int64) | OCCURS(int32)  |
+-----------------+---------------+----------------+
|       16        |       8       |        4       |
+-----------------+---------------+----------------+
```

我们不保存行的原值而是保存其哈希值，因此如果文件中的行都非常短，占用的空间会比较大。

### 步骤2: 聚合

具有相同哈希值的行将保存到同一文件中。 因此，我们可以在同一个小文件中汇总它的出现次数。

在 `internal / minheap.go` 中有一个线程安全的最小堆 `MsgMinHeap`。 该堆是大小固定的，当堆足够大时（大于配置文件中的 N），它仅压入 occurs 值比堆顶元素大的行，并在插入后弹出堆顶元素。

程序将会用多个协程依次读取并汇总小文件里的数据，并将汇总后的数据推送给`MsgMinHeap`。

最后，堆中的所有元素就是我们要求的前 N 个出现次数最多的行。 我们使用偏移量（offset）在源文件中找到原始字符串。

## 内存使用

每个 Reduce 任务都会维护一个容量为 1024 的 map，map 的 key 是哈希值，value 是 Msg 结构体的指针：

```Go
type Msg struct {
	data   []byte
	hash   []byte
	offset int64
	occurs int
}
```

在 hash value 计算完成后会将 data 置为 nil。实际上通过 unsafe.sizeof 算出来的 Msg 占用的空间是 64 bytes。加上 key 的 16 bytes 就是 80 bytes。

假如有 32 个协程运行 Reduce 任务（这个值可以通过 conf.json 中的 concurrents 字段配置）那么实际占用的内存空间为 80 * 1024 * 32 = 2.5 MB。可以通过增加协程来增大处理和写入效率，但是使用的内存也会更多。

在汇总时默认使用 10 个协程读文件，并且用了一个原子操作保证它们不会重复读相同的文件。对于一个 800 万行的大文件，切割成 2048 份后每个小文件大小为 8000000 * 28 / 2048 = 106 kb，因此 10 个协程同时将单个小文件载入内存，也就占用了 1MB 多的内存。

不过实际上 Go 里读文件缓冲什么的还会释放很多内存，具体什么时候触发垃圾回收把读入的行数据清除掉也不是我们控制的，所以实际上占用内存会比计算的大一些。

## 性能

测试文件：800万行随机URL，30G

在我的 Macbook 2018 上，在3分钟内获得出现次数前 100 的行。并且内存使用量不超过 200 MB  。


---

This program can find the most frequently occurring lines of a large file. For example, a file contains millions URLs.

## How it works

It is easy to find the most frequently occuring line in a file. But it would be hard when the file is too large that you can not load it to memory.

This program split the large file into many small files, and then aggregate them to find the answers.

### STEP 1: Split file

I use the idea of map reduce here:

Map task read each line from source file, and send lines to Reduce tasks via channels. We only have one Map task because reading a file line by line is fast.

Reduce tasks receive lines, calculate hash value and save lines to a go map. When the map size is large enough, it will save information about lines to a small binary file. There are many binary files and reduce task will choose the binary file by the line's hash value. For example, if a line's hash value is K, then it will be saved to file (K mod splitNum). Therefore, same lines will be saved to the same file.

Each line will be saved in following format:

```
+-----------------+---------------+----------------+
| HASH VALUE(MD5) | OFFSET(int64) | OCCURS(int32)  |
+-----------------+---------------+----------------+
|       16        |       8       |        4       |
+-----------------+---------------+----------------+
```

We save the hash value and offset but not the origin line here. Therefore, it would occupy too much space in disk when lines are short.

### STEP 2: Aggregate

Lines with same hash value are saved to the same file. So we can count its occuring times in just one binary file. 

There is a thread safe min heap `MsgMinHeap` in `internal/minheap.go`. This heap is size fixed, when the heap is large enough, it only accepts lines that have bigger occuring times than the minimum item in the heap, and pop the minimum item after insertion. 

After counting occuring times in a binary file, we will push the line with its occuring times to the heap.

Finally, all items in the heap are what we want. We use the offset to find the origin string in the source file.

## Memory Usage

Every Reduce task maintain a go map, in 1024 capacity. The key is hash value of the line and the value is a Msg pointer:

```Go
type Msg struct {
	data   []byte
	hash   []byte
	offset int64
	occurs int
}
```

`data` will be set as nil after we get the hash value. 

## Performance

Test File: 8 million lines random urls, 30G (don't have more disk space)

Get top 100 most frequently occurring lines in 3 minutes on my macbook 2018. And no more than 200MB memory usage.

## Any better ideas?

We can use data structure like B-Tree to aggregate data in disk, and then we can skip STEP 2. But it needs to write a B-Tree or import third-party packages.