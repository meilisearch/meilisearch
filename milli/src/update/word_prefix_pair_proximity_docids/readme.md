## What is WordPrefixPairProximityDocids?
The word-prefix-pair-proximity-docids database is a database whose keys are of the form (`word`, `prefix`, `proximity`) and the values are roaring bitmaps of the documents which contain `word` followed by another word starting with `prefix` at a distance of `proximity`.

The prefixes present in this database are only those that correspond to many different words in the documents.

## How is it created/updated? (simplified version)
To compute it, we have access to (mainly) two inputs:

* a list of sorted prefixes, such as:
```
c
ca
cat
d
do
dog
```
Note that only prefixes which correspond to more than a certain number of different words from the database are included in this list.

* a sorted list of word pairs and the distance between them (i.e. proximity), associated with a roaring bitmap, such as:
```
good dog   3         -> docids1: [2, 5, 6]
good doggo 1		 -> docids2: [8]
good dogma 1		 -> docids3: [7, 19, 20]
good ghost 2 		 -> docids4: [1]
horror cathedral 4	 -> docids5: [1, 2]
```

I illustrate a simplified version of the algorithm to create the word-prefix-pair-proximity database below:

1. **Outer loop:** First, we iterate over each word pair and its proximity:
```
word1    : good
word2    : dog
proximity: 3
```
2. **Inner loop:** Then, we iterate over all the prefixes of `word2` that are in the list of sorted prefixes. And we insert the key (`prefix`, `proximity`) and the value (`docids`) to a sorted map which we call the “batch”. For example, at the end of the first inner loop, we may have:
```
Outer loop 1:
------------------------------
word1    : good
word2    : dog
proximity: 3
docids   : docids1

prefixes: [d, do, dog]

batch: [
	(d, 3)   -> [docids1]
	(do, 3)  -> [docids1]
	(dog, 3) -> [docids1]
]
```
3. For illustration purpose, let's run through a second iteration of the outer loop:
```
Outer loop 2:
------------------------------
word1    : good
word2    : doggo
proximity: 1
docids   : docids2

prefixes: [d, do, dog]

batch: [
	(d, 1)   -> [docids2]
	(d, 3)   -> [docids1]
	(do, 1)  -> [docids2]
	(do, 3)  -> [docids1]
	(dog, 1) -> [docids2]
	(dog, 3) -> [docids1]
]
```
Notice that the batch had to re-order some (`prefix`, `proximity`) keys: some of the elements inserted in the second iteration of the outer loop appear *before* elements from the first iteration.

4. And a third:
```
Outer loop 3:
------------------------------
word1    : good
word2    : dogma
proximity: 1
docids   : docids3

prefixes: [d, do, dog]

batch: [
	(d, 1)   -> [docids2, docids3]
	(d, 3)   -> [docids1]
	(do, 1)  -> [docids2, docids3]
	(do, 3)  -> [docids1]
	(dog, 1) -> [docids2, docids3]
	(dog, 3) -> [docids1]
]
```
Notice that there were some conflicts which were resolved by merging the conflicting values together.

5. On the fourth iteration of the outer loop, we have:
```
Outer loop 4:
------------------------------
word1    : good
word2    : ghost
proximity: 2
```
Because `word2` begins with a different letter than the previous `word2`, we know that:
1. All the prefixes of `word2` are greater than the prefixes of the previous word2
2. And therefore, every instance of (`word2`, `prefix`) will be greater than any element in the batch.
Therefore, we know that we can insert every element from the batch into the database before proceeding any further. This operation is called “flushing the batch”. Flushing the batch should also be done whenever `word1` is different than the previous `word1`.

6. **Flushing the batch:** to flush the batch, we look at the `word1` and iterate over the elements of the batch in sorted order:
```
Flushing Batch loop 1:
------------------------------
word1    : good
word2    : d
proximity: 1
docids   : [docids2, docids3]
```
We then merge the array of `docids` (of type `Vec<Vec<u8>>`) using `merge_cbo_roaring_bitmap` in order to get a single byte vector representing a roaring bitmap of all the document ids where `word1` is followed by `prefix` at a distance of `proximity`.
Once we have done that, we insert (`word1`, `prefix`, `proximity`) -> `merged_docids` into the database.

7. That's it! ... except...

## How is it created/updated (continued)

I lied a little bit about the input data. In reality, we get two sets of the inputs described above, which come from different places:

* For the list of sorted prefixes, we have: 
	* `new_prefixes`, which are all the prefixes that were not present in the database before the insertion of the new documents
	* `common_prefixes` which are the prefixes that are present both in the database and in the newly added documents

* For the list of word pairs and proximities, we have:
	* `new_word_pairs`, which is the list of word pairs and their proximities present in the newly added documents
	* `word_pairs_db`, which is the list of word pairs from the database. **This list includes all elements in `new_word_pairs`** since `new_word_pairs` was added to the database prior to calling the `WordPrefixPairProximityDocIds::execute` function.

To update the prefix database correctly, we call the algorithm described earlier first on (`common_prefixes`, `new_word_pairs`) and then on (`new_prefixes`, `word_pairs_db`). Thus:

1. For all the word pairs that were already present in the DB, we insert them again with the `new_prefixes`. Calling the algorithm on them with the `common_prefixes` would not result in any new data.
3. For all the new word pairs, we insert them twice: first with the `common_prefixes`, and then, because they are part of `word_pairs_db`, with the `new_prefixes`. 

Note, also, that since we read data from the database when iterating over `word_pairs_db`, we cannot insert the computed word-prefix-pair-proximity-docids from the batch directly into the database (we would have a concurrent reader and writer). Therefore, when calling the algorithm on (`new_prefixes`, `word_pairs_db`), we insert the computed ((`word`, `prefix`, `proximity`), `docids`) elements in an intermediary grenad Writer instead of the DB. At the end of the outer loop, we finally read from the grenad and insert its elements in the database.


