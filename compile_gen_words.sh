#sudo yum install bzip2 bzip2-devel
#sudo yum install libzstd-devel
#sudo yum install zlib zlib-devel
#sudo yum install snappy snappy-devel
#sudo yum install lz4-devel
if [ ! -f "sqlite-amalgamation-3390400/sqlite3.c" ]; then
  curl -o sqlite3.39.04.zip https://www.sqlite.org/2022/sqlite-amalgamation-3390400.zip
  rm -rf sqlite-amalgamation-3390400/
  unzip sqlite3.39.04.zip
  rm -f sqlite3.o
fi
if [ ! -f "sqlite3.h" ]; then
  cp sqlite-amalgamation-3390400/sqlite3.h .
fi
if [ ! -f "sqlite3.o" ]; then
  gcc -c sqlite-amalgamation-3390400/sqlite3.c -o sqlite3.o
fi
if [ ! -f "RC_2013-04.zst" ]; then
  curl -o RC_2013-04.zst https://files.pushshift.io/reddit/comments/RC_2013-04.zst
fi
if [ ! -f "lid.176.bin" ]; then
  curl -o lid.176.bin https://dl.fbaipublicfiles.com/fasttext/supervised-models/lid.176.bin
fi
COMP_OPTS="-O3 -std=c++17"
# -fsanitize=address"
g++ ${COMP_OPTS} -L/usr/local/lib -I. -Imisc -I../mimalloc/include -I/usr/local/include -I../fasttext/src -I../bloom/src -I../lmdb/libraries/liblmdb gen_words_and_phrases.cpp sqlite3.o /usr/local/lib/libzstd.a ../fasttext/libfasttext.a ../index_research/src/basix.cpp ../index_research/src/bfos.cpp ../index_research/src/univix_util.cpp ../bloom/src/bloom.c /usr/local/lib/librocksdb.a ../lmdb/libraries/liblmdb/liblmdb.a /usr/local/lib/libwiredtiger.a -lpthread -ldl -llz4 -lbz2 -lzstd -lz -lsnappy -lbrotlienc
#-fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free -ljemalloc

# screen sh -c './a.out RC_2013-04.zst 5000 4096 rocksdb >> logs/aws_m6gd/rocksdb_320mb_22dec.log 2>>fail.log '
