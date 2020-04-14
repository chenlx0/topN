# topN

This program can find the most frequently occurring lines of a large file. For example, a file contains millions URLs.

## How it works

It is easy to find the most frequently occuring line in a file. But it would be hard when the file is too large that you can not load it to memory.

This program split the large file into many small files, and then aggregate them to find the answers.

### STEP 1: Split file

I use the idea of map reduce here:

Map task read each line from source file, and send lines to Reduce tasks via channels.

Reduce tasks receive lines, calculate hash value and save lines to a go map. When the map size is large enough, it will save information about lines to a small binary file. There are many binary files and reduce task will choose the binary file by the line's hash value. So, same lines will be saved to the same file.

It would not save lines with their origin strings. Each line will be saved in following format:

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

## Any better ideas?

We can use data structure like B-Tree to aggregate data in disk, and then we can skip STEP 2. But it needs to write a B-Tree or import third-party packages.
