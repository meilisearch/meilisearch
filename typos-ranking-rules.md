# Typo and Ranking rules

This is an explanation of the default rules used in MeiliSearch.

First we have to explain some terms that are used in this reading.

- A query string is the full list of all the words that the end user is searching for results.
- A query word is one of the words that compose the query string.



## Typo rules

The typo rules are used before sorting the documents. They are used to aggregate them, to choose which documents contain words similar to the queried words.

We use a prefix _Levenshtein_ algorithm to check if the words match. The only difference with a Levenshtein algorithm is that it accepts every word that **starts with the query words** too. Therefore words are accepted if they start with or have the equal length.



The Levenshtein distance between two words _M_ and _P_ is called "the minimum cost of transforming _M_ into _P_" by performing the following elementary operations:

- substitution of a character of _M_ by a character other than _P_. (e.g. **k**itten → **s**itten)
- insertion in _M_ of a character of _P_. (e.g. sittin → sittin**g**)
- deleting a character from _M_. (e.g. satu**r**day → satuday)



There are some rules about what can be considered "similar". These rules are **by word** and not for the whole query string.

- If the query word is between 1 and 4 characters long therefore **no** typo is allowed, only documents that contains words that start or are exactly equal to this query word are considered valid for this request.
- If the query word is between 5 and 8 characters long, **one** typo is allowed. Documents that contains words that match with one typo are retained for the next steps.
- If the query word contains more than 8 characters, we accept a maximum of **two** typos.



This means that "satuday", which is 7 characters long, use the second rule and every document containing words that have only **one** typo will match. For example:

- "satuday" is accepted because it is exactly the same word.
- "sat" is not accepted because the query word is not a prefix of it but the opposite.
- "satu**r**day" is accepted because it contains **one** typo.
- "s**u**tu**r**day" is not accepted because it contains **two** typos.



## Ranking rules

All documents that have been aggregated using the typo rules above can now be sorted. MeiliSearch uses a bucket sort.

What is a bucket sort? We sort all the documents with the first rule, for all documents that can't be separated we create a group and sort it using the second rule, and so on.

Here is the list of all the default rules that are executed in this specific order by default:

- _Typo_ - The less typos there are beween the query words and the document words, the better is the document.
- _Words_ - A document containing more of the query words will be more important than one that contains less.
- _Proximity_ - The closer the query words are in the document the better is the document.
- _Attribute_ - A document containing the query words in a more important attribute than another document is considered better.
- _Words Position_ - A document containing the query words at the start of an attribute is considered better than a document that contains them at the end.
- _Exactness_ - A document containing the query words in their exact form, not only a prefix of them, is considered better.

