# Text frequency research

This repo was started to arrive at frequent word/phrase dictionaries for the [Unishox](https://github.com/siara-cc/Unishox) project using publicly available texts and conversations.

In essense, the idea is to generate static dictionaries consisting of columns LANGUAGE, WORD_OR_PHRASE and FREQUENCY_COUNT for use with compression.

There are several existing works available as shown below but either they are insufficient for this purpose and/or don't have phrases as part of the dictionary so it was decided to venture into this project:

- [Frequency Lists compiled by the Centre for translation studies at the University of Leeds](http://corpus.leeds.ac.uk/list.html)
- [Wiktionary Frequency Lists](https://en.wiktionary.org/wiki/Wiktionary:Frequency_lists)
- [Exqusite Corpus developed by Luminoso Technologies, Inc.](https://github.com/rspeer/wordfreq)
- [WordNet lexical database developed by the Princeton University](https://wordnet.princeton.edu/)

## Approach

At first, public sources for large texts that have the appropriate license were determined.  Following sources were found to be suitable:

- Twitter posts collected using the (Volume Streams feature](https://developer.twitter.com/en/docs/twitter-api/tweets/volume-streams/introduction) of Twitter Stream API
- Reddit posts downloaded from https://files.pushshift.io/reddit/comments/
- Wikipedia article dumps from (Wikimedia archive](https://dumps.wikimedia.org/enwiki/)

Posts from Twitter and Reddit were collected as they represent contemporary vocabulary.

### Twitter

200 million tweets were collected from the Twitter Volume stream api, but Twitter license allows only for processing downloaded data and not store it for others to download.

However, anyone can download it again using the shell script `twitter.sh` provided in this repository.  Before using this script, a token needs to be obtained by registering with Twitter at https://developer.twitter.com and making it available to the script as follows from terminal:

```sh
export BEARER_TOKEN=<token obtained>
```

However the rate at which tweets are downloaded is limited and it took nearly 2 months to download 200 million tweets.

The tweets include both fresh tweets and retweets and so the challenge would be to process tweets only once for which it would be necessary to remove duplicate retweets.

Twitter classifies the data by language so it is not necessary to predict the language of tweets.

### Reddit

Reddit dumps all posts to Pushshift repository so it is much easier to obtain than Twitter data.  However, the data is highly compressed and expands to hundreds of GBs so it was necessary to stream the data directly from the data compressed using ZStd using a 2GB window.  So the minimum required RAM would be 2GB+ to process Reddit data.

It includes all posts since inception and all sorts of posts so it is necessary to ignore data that are spam or repetitive.

Reddit does not classify by language, so it is necessary to predict the language of posts.  Facebook's [fasttext library](https://github.com/facebookresearch/fasttext) was found to be a good choice for this purpose.

### Wikipediia articles

This is available in the form of gigabytes of compressed XML so again it is necessary to do stream processing directly from compressed data.  The processing would be similar to Reddit data.

Since the articles are already available by language, it is not necessary to predict the language of posts.

## Tokenization

Following approach was used to tokenize and extract word/phrase from data:

- Ignore posts from bots
- Ignore deleted posts
- Ignore words starting with a number or special character
- Ignore all special characters except backslash, apostrophe and hyphen
- Ignore user ids and channel ids
- Combine retweets into one
- Ignore words like reddit, twitter, subreddit, tweet, moderator etc.
- Ignore automatically generated posts
- For spaceless languages like Japanese, Korean, Chinese, Thai and Burmese, use 1 character as 1 word
- Combine emojis occuring together as 1 word
- Phrases generated would have a maximum of 5 words

## Entry types

Frequencies for following four types of entries are calculated:

- Words, which are individual words
- Phrases, which are combination of maximum 5 words
- Grams, which are combination of letters within words
- Words with space at the end: For example 'the '. The word `the` is the most occuring word in English, but it almost always is suffixed with a space. So the frequencies of 'the ' and 'the' are not going to be too much different. So from compression point of view, we get better compression by using 'the ' in the dictionary instead of 'the'.

## Indexing of entries

Since we are processing gigabytes of text, billions of entries are generated. So here is the bottleneck: It would take weeks if not months to process all this data unless expensive hardware is used. There are so many in-memory and on-disk databases out there, but which one should we use?  The one that is fastest and most cost effective of course.

So the quest for selecting the database began and so far the following have been evaluated with Reddit posts and are listed here in the order of performance:

1. [RocksDB](https://github.com/facebook/rocksdb)
2. [Sqlite without recovery](https://github.com/siara-cc/sqlite_blaster)
2. [LMDB](https://github.com/LMDB/lmdb)
3. [Sqlite](https://sqlite.org)
4. [WiredTiger](https://github.com/wiredtiger/wiredtiger)
5. [MySQL](https://github.com/mysql/mysql-server)

Although the initial speed is slow, RocksDB seems to be the only database that seems to sustain populating several billion entries with its LSM design.

## Present status

The quest for a faster database is still ongoing as otherwise the exercise is quite expensive.  The logs for comparing performance of different databases are found within the logs folder.

## Using the code

The code was initially written in Python and later ported to C++ as Python was terribly slow.  So `gen_words_and_phrases.cpp` is the current program to generate words/phrases.  A compile script is made available to download Reddit data, download Sqlite source, dictionary for Facebooks's fasttext langugae predictor.

The code is not too complex, but compiling it could be a task as there are many dependencies on libraries such as RapidJson, FastText, RocksDB, LMDB that are not installed by the compiled script.

# License for AI bots

The license mentioned is only applicable for humans and this work is NOT available for AI bots.

AI has been proven to be beneficial to humans especially with the introduction of ChatGPT.  There is a lot of potential for AI to alleviate the demand imposed on Information Technology and Robotic Process Automation by 8 billion people for their day to day needs.

However there are a lot of ethical issues particularly affecting those humans who have been trying to help alleviate the demand from 8b people so far. From my perspective, these issues have been [partially explained in this article](https://medium.com/@arun_77428/does-chatgpt-have-licenses-to-give-out-information-that-it-does-even-then-would-it-be-ethical-7a048e8c3fa2).

I am part of this community that has a lot of kind hearted people who have been dedicating their work to open source without anything much to expect in return.  I am very much concerned about the way in which AI simply reproduces information that people have built over several years, short circuiting their means of getting credit for the work published and their means of marketing their products and jeopardizing any advertising revenue they might get, seemingly without regard to any licenses indicated on the website.

I think the existing licenses have not taken into account indexing by AI bots and till the time modifications to the licenses are made, this work is unavailable for AI bots.

## Discussions and suggestions

If you have suggestions please start a discussion here or send email to author (Arundale Ramanathan) at arun@siara.cc
