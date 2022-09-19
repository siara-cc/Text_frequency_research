if [ ! -f "sqlite3.o" ]; then
  gcc -c sqlite3.c
fi
if [ ! -f "RC_2013-04.zst" ]; then
  curl -o RC_2013-04.zst https://files.pushshift.io/reddit/comments/RC_2013-04.zst
fi
g++ -std=c++11 -O3 -I. -I../mimalloc/include -I/usr/local/include -I./cloud/fastText-0.9.2/src gen_words_and_phrases.cpp sqlite3.o /usr/local/lib/libzstd.a ./cloud/fastText-0.9.2/libfasttext.a ../index_research/src/basix.cpp ../index_research/src/bfos.cpp ../index_research/src/univix_util.cpp -lpthread -ldl

