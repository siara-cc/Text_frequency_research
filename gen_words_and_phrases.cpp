/*
 * Copyright (c) Yann Collet, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under both the BSD-style license (found in the
 * LICENSE file in the root directory of this source tree) and the GPLv2 (found
 * in the COPYING file in the root directory of this source tree).
 * You may select, at your option, one of the above-listed licenses.
 */

int INSERT_INTO_IDX = 0;
int INSERT_INTO_SQLITE_BLASTER = 0;
int INSERT_INTO_SQLITE = 0;
int INSERT_INTO_LMDB = 0;
int INSERT_INTO_ROCKSDB = 0;
int INSERT_INTO_WT = 0;
int GEN_SQL = 0;

#include <stdio.h>     // fprintf
#include <stdlib.h>
#include <zstd.h>      // presumes zstd library is installed
#include <fasttext.h>
#include "common.h"    // Helper functions, CHECK(), and CHECK_ZSTD()
#include <string>
#include <sstream>
#include <rapidjson/document.h>
#include <sqlite3.h>
#include <codecvt>
#include <locale>
#include <chrono>
#include <termios.h>
#include <sys/ioctl.h>
#include <sys/types.h>
#include <sys/resource.h>
#include "../sqlite_blaster/src/sqlite_index_blaster.h"
//#include "../index_research/src/sqlite.h"
//#include "../index_research/src/stager.h"

#if defined(__APPLE__)
#include <libproc.h>
#endif

#include <sys/resource.h>
#include <unistd.h>

#include "rocksdb/db.h"
#include "rocksdb/slice.h"
#include "rocksdb/options.h"
#include "rocksdb/cache.h"
#include "rocksdb/table.h"
#include "rocksdb/rate_limiter.h"
#include "rocksdb/advanced_options.h"

#include "lmdb.h"

//\#include "wiredtiger.h"

using ROCKSDB_NAMESPACE::DB;
using ROCKSDB_NAMESPACE::Options;
using ROCKSDB_NAMESPACE::PinnableSlice;
using ROCKSDB_NAMESPACE::ReadOptions;
using ROCKSDB_NAMESPACE::Status;
using ROCKSDB_NAMESPACE::WriteBatch;
using ROCKSDB_NAMESPACE::WriteOptions;

using namespace std;
using namespace chrono;
using namespace rocksdb;

std::string kDBPath = "./rocksdb_word_freq";
DB* rocksdb1;
Options rdb_options;

// LMDB
MDB_env *env;
MDB_dbi dbi;
MDB_val mdb_key, mdb_data;
MDB_txn *txn;
MDB_cursor *cursor;

time_point<steady_clock> start;
fasttext::FastText ftext;
string remain_buf;
size_t line_count = 0;
size_t lines_processed = 0;
sqlite3 *db;
char *zErrMsg = 0;
int rc;
sqlite3_stmt *ins_word_freq_stmt;
sqlite3_stmt *sel_word_freq_stmt;
sqlite3_stmt *upd_word_freq_stmt;
sqlite3_stmt *del_word_freq_stmt;
const char *tail;
wstring_convert<codecvt_utf8<wchar_t>> myconv;
sqlite_index_blaster *ix_obj;
//stager *ix_obj;
int start_at = 0;

FILE *log_fp;
int log_fd;

int kbhit() {
    static const int STDIN = 0;
    static int initialized = 0;

    if (! initialized) {
        // Use termios to turn off line buffering
        struct termios term;
        tcgetattr(STDIN, &term);
        term.c_lflag &= ~ICANON;
        tcsetattr(STDIN, TCSANOW, &term);
        setbuf(stdin, NULL);
        initialized = 1;
    }

    int bytesWaiting;
    ioctl(STDIN, FIONREAD, &bytesWaiting);
    return bytesWaiting;
}

void printPredictions(
    const vector<pair<fasttext::real, string>>& predictions,
    bool printProb,
    bool multiline) {
  bool first = true;
  for (const auto& prediction : predictions) {
    if (!first && !multiline) {
      cout << " ";
    }
    first = false;
    cout << prediction.second;
    if (printProb) {
      cout << " " << prediction.first;
    }
    if (multiline) {
      cout << endl;
    }
  }
  if (!multiline) {
    cout << endl;
  }
}

int max_word_len = 0;
int words_generated = 0;
int words_inserted = 0;
int words_updated1 = 0;
int words_updated2 = 0;
int num_words = 0;
int num_phrases = 0;
int num_grams = 0;
long total_word_lens = 0;
void insert_into_db(const char *utf8word, int word_len, const char *lang_code, const char *is_word, const char *source) {

    if (!INSERT_INTO_SQLITE)
      return;

    sqlite3_reset(sel_word_freq_stmt);
    sqlite3_bind_text(sel_word_freq_stmt, 1, lang_code, strlen(lang_code), SQLITE_STATIC);
    sqlite3_bind_text(sel_word_freq_stmt, 2, utf8word, word_len, SQLITE_STATIC);
    if (sqlite3_step(sel_word_freq_stmt) == SQLITE_ROW) {
        int count = sqlite3_column_int(sel_word_freq_stmt, 0);
        sqlite3_reset(upd_word_freq_stmt);
        sqlite3_bind_text(upd_word_freq_stmt, 1, is_word, 1, SQLITE_STATIC);
        sqlite3_bind_text(upd_word_freq_stmt, 2, lang_code, strlen(lang_code), SQLITE_STATIC);
        sqlite3_bind_text(upd_word_freq_stmt, 3, utf8word, word_len, SQLITE_STATIC);
        if (sqlite3_step(upd_word_freq_stmt) != SQLITE_DONE) {
            fprintf(stderr, "Error updating data: %s\n", sqlite3_errmsg(db));
            //sqlite3_close(db);
            return;
        }
        words_updated1++;
    } else {
        sqlite3_reset(ins_word_freq_stmt);
        sqlite3_bind_text(ins_word_freq_stmt, 1, lang_code, strlen(lang_code), SQLITE_STATIC);
        sqlite3_bind_text(ins_word_freq_stmt, 2, utf8word, word_len, SQLITE_STATIC);
        sqlite3_bind_int(ins_word_freq_stmt, 3, 41);
        sqlite3_bind_text(ins_word_freq_stmt, 4, is_word, 1, SQLITE_STATIC);
        sqlite3_bind_text(ins_word_freq_stmt, 5, source, 1, SQLITE_STATIC);
        if (sqlite3_step(ins_word_freq_stmt) != SQLITE_DONE) {
            fprintf(stderr, "Error inserting data 1: %s\n", sqlite3_errmsg(db));
            //sqlite3_close(db);
            return;
        }
        words_inserted++;
    }

}

void write_uint32(uint8_t *ptr, uint32_t input) {
    int i = 4;
    while (i--)
        *ptr++ = (input >> (8 * i)) & 0xFF;
}

uint32_t read_uint32(const uint8_t *ptr) {
    uint32_t ret;
    ret = ((uint32_t)*ptr++) << 24;
    ret += ((uint32_t)*ptr++) << 16;
    ret += ((uint32_t)*ptr++) << 8;
    ret += *ptr;
    return ret;
}

void insert_into_idx(const char *utf8word, int word_len, const char *lang_code, const char *is_word, const char *source) {
    //cout << "[" << utf8word << "]" << endl;
    //return;
    if (!INSERT_INTO_IDX)
      return;
    int value_len = 4;
    uint32_t count = 1;
    char key[400];
    uint8_t val[5];
    strcpy(key, lang_code);
    strcat(key, " ");
    strcat(key, utf8word);
    word_len++;
    word_len += strlen(lang_code);
    bool is_found = ix_obj->get((const uint8_t *) key, (uint8_t) word_len, &value_len, val);
    if (is_found) {
        count = read_uint32(val);
        count++;
        write_uint32(val, count);
        words_updated1++;
    } else {
        words_inserted++;
        total_word_lens += word_len;
        value_len = 4;
        write_uint32(val, 1);
    }
    ix_obj->put((const uint8_t *) key, (uint8_t) word_len, val, 4);
    if (word_len > max_word_len)
        max_word_len = word_len;
}

void insert_into_sqlite_blaster(const char *utf8word, int word_len, const char *lang_code, const char *is_word, const char *source) {
    //cout << "[" << utf8word << "]" << endl;
    //return;
    if (!INSERT_INTO_SQLITE_BLASTER)
      return;
    int32_t count = 1;
    int expected_rec_len = strlen(lang_code) + word_len + sizeof(int32_t) + strlen(is_word) + strlen(source) + 8;
    uint8_t sqlite_rec[expected_rec_len];
    uint8_t sqlite_rec_read[expected_rec_len];
    const size_t value_lens[] = {strlen(lang_code), (size_t) word_len, sizeof(int32_t), strlen(is_word), strlen(source)};
    const uint8_t col_types[] = {SQLT_TYPE_TEXT, SQLT_TYPE_TEXT, SQLT_TYPE_INT32, SQLT_TYPE_TEXT, SQLT_TYPE_TEXT};
    int rec_len = ix_obj->make_new_rec(sqlite_rec, 5, (const void *[]) {lang_code, utf8word, &count, is_word, source}, value_lens, col_types);
    int rec_read_len = expected_rec_len;
    bool is_found = ix_obj->get(sqlite_rec, -rec_len, &rec_read_len, sqlite_rec_read);
    if (is_found) {
        ix_obj->read_col(2, sqlite_rec_read, rec_read_len, &count);
        count++;
        rec_len = ix_obj->make_new_rec(sqlite_rec, 5, (const void *[]) {lang_code, utf8word, &count, is_word, source}, value_lens, col_types);
        words_updated1++;
    } else {
        words_inserted++;
        total_word_lens += word_len;
    }
    ix_obj->put(sqlite_rec, -rec_len, NULL, 0);
    if (word_len > max_word_len)
        max_word_len = word_len;
}

void insert_into_lmdb(const char *utf8word, int word_len, const char *lang_code, const char *is_word, const char *source) {
    //cout << "[" << utf8word << "]" << endl;
    //return;
    if (!INSERT_INTO_LMDB)
      return;
    int16_t value_len;
    uint32_t count = 1;
    char key[400];
    string count_str("1");
    strcpy(key, lang_code);
    strcat(key, " ");
    strcat(key, utf8word);
    //cout << "Key: " << key << endl;
    mdb_key.mv_size = strlen(key);
    mdb_key.mv_data = key;
    rc = mdb_get(txn, dbi, &mdb_key, &mdb_data);
    if (rc != 0 && rc != MDB_NOTFOUND) {
        fprintf(stderr, "mdb_get: (%d) %s\n", rc, mdb_strerror(rc));
        return;
    }
    if (rc == 0) {
      //cout << "found: " << count_str << endl;
      count = *((uint32_t *) mdb_data.mv_data);
      count++;
      words_updated1++;
    } else {
      //cout << "not found: " << endl;
      count = 1;
      words_inserted++;
      total_word_lens += word_len;
    }
    count_str = std::to_string(count);
    mdb_data.mv_size = sizeof(uint32_t);
    mdb_data.mv_data = &count;
    rc = mdb_put(txn, dbi, &mdb_key, &mdb_data, 0);
    if (rc) {
        fprintf(stderr, "mdb_put: (%d) %s\n", rc, mdb_strerror(rc));
        return;
    }
    //cout << "Put complete " << endl;
    if (word_len > max_word_len)
      max_word_len = word_len;
}

void insert_into_rocksdb(const char *utf8word, int word_len, const char *lang_code, const char *is_word, const char *source) {
    //cout << "[" << utf8word << "]" << endl;
    //return;
    if (!INSERT_INTO_ROCKSDB)
      return;
    int16_t value_len;
    uint32_t count = 1;
    char key[400];
    string count_str("1");
    strcpy(key, lang_code);
    strcat(key, " ");
    strcat(key, utf8word);
    //cout << "Key: " << key << endl;
    Status s = rocksdb1->Get(ReadOptions(), key, &count_str);
    if (!s.IsNotFound()) {
      //cout << "found: " << count_str << endl;
      count = atoi(count_str.c_str());
      count++;
      words_updated1++;
    } else {
      //cout << "not found: " << endl;
      count = 1;
      words_inserted++;
      total_word_lens += word_len;
    }
    count_str = std::to_string(count);
    s = rocksdb1->Put(WriteOptions(), key, count_str);
    assert(s.ok());
    //cout << "Put complete " << endl;
    if (word_len > max_word_len)
      max_word_len = word_len;
}
/*
WT_CONNECTION *wt_conn;
WT_CURSOR *wt_cursor;
WT_SESSION *wt_session;
void insert_into_wt(const char *utf8word, int word_len, const char *lang_code, const char *is_word, const char *source) {
    //cout << "[" << utf8word << "]" << endl;
    //return;
    if (!INSERT_INTO_WT)
      return;
    int16_t value_len;
    uint32_t count = 1;
    char key[400];
    string count_str("1");
    strcpy(key, lang_code);
    strcat(key, " ");
    strcat(key, utf8word);
    //cout << "Key: " << key << endl;
    wt_cursor->set_key(wt_cursor, key);
    if (wt_cursor->search(wt_cursor) == 0) {
      //cout << "found: " << count_str << endl;
      wt_cursor->get_value(wt_cursor, &count);
      count++;
      wt_cursor->set_value(wt_cursor, count);
      wt_cursor->update(wt_cursor);
      words_updated1++;
    } else {
      //cout << "not found: " << endl;
      count = 1;
      words_inserted++;
      total_word_lens += word_len;
      wt_cursor->set_value(wt_cursor, count);
      wt_cursor->insert(wt_cursor);
    }
    wt_cursor->reset(wt_cursor);
    //cout << "Put complete " << endl;
    if (word_len > max_word_len)
      max_word_len = word_len;
}
*/
void output_sql(string& utf8word, int len, const char *lang_code, const char *is_word, const char *source) {
    if (!GEN_SQL)
      return;
    string ins_sql = "INSERT INTO word_freq (word, lang, count, is_word, source) VALUES ('";
    string utf8word_encoded;
    size_t sq_loc = utf8word.find('\'');
    int start_pos = 0;
    while (sq_loc != string::npos) {
        utf8word_encoded.append(utf8word.substr(start_pos, sq_loc - start_pos));
        utf8word_encoded.append("''");
        start_pos = ++sq_loc;
        sq_loc = utf8word.find('\'', start_pos);
    }
    utf8word_encoded.append(utf8word.substr(start_pos));
    ins_sql.append(utf8word_encoded);
    ins_sql.append("', '");
    ins_sql.append(lang_code);
    ins_sql.append("', 1, '");
    ins_sql.append(is_word);
    ins_sql.append("', '");
    ins_sql.append(source);
    ins_sql.append("') ON CONFLICT DO UPDATE SET count = count + 1, , is_word = iif(is_word='y', 'y', excluded.is_word), "
                             "source = iif(instr(source, 'r') = 0, source||'r', source);");
    cout << ins_sql << endl;

}

void insert_data(char *lang_code, wstring& word, const char *is_word, int max_ord) {

    int word_len = word.length();
    if (word_len == 0 || word[word_len-1] == '~')
        return;
    if (word_len == 2 && max_ord < 128)
        return;
    if (word_len == 1 && max_ord < 4256)
        return;

    words_generated++;

    string utf8word = myconv.to_bytes(word);

    fprintf(log_fp, "[%s], [%s], %d\n", lang_code, utf8word.c_str(), utf8word.length());
    fflush(log_fp);
    //fsync(log_fd);

    // cout << "[" << utf8word << "], " << word.length() << endl;

    insert_into_db(utf8word.c_str(), utf8word.length(), lang_code, is_word, "r");
    insert_into_idx(utf8word.c_str(), utf8word.length(), lang_code, is_word, "r");
    insert_into_sqlite_blaster(utf8word.c_str(), utf8word.length(), lang_code, is_word, "r");
    insert_into_lmdb(utf8word.c_str(), utf8word.length(), lang_code, is_word, "r");
    insert_into_rocksdb(utf8word.c_str(), utf8word.length(), lang_code, is_word, "r");
    //insert_into_wt(utf8word.c_str(), utf8word.length(), lang_code, is_word, "r");
    output_sql(utf8word, utf8word.length(), lang_code, is_word, "r");
    if (is_word[0] == 'y') {
        size_t spc_loc = utf8word.find(' ');
        if (spc_loc != string::npos && word.find(' ', spc_loc+1) == string::npos && spc_loc > 1) {
            string word_with_spc = utf8word.substr(0, spc_loc+1);
            insert_into_db(word_with_spc.c_str(), word_with_spc.length(), lang_code, "n", "r");
            insert_into_idx(word_with_spc.c_str(), word_with_spc.length(), lang_code, "n", "r");
            insert_into_sqlite_blaster(word_with_spc.c_str(), word_with_spc.length(), lang_code, "n", "r");
            insert_into_lmdb(word_with_spc.c_str(), word_with_spc.length(), lang_code, "n", "r");
            insert_into_rocksdb(word_with_spc.c_str(), word_with_spc.length(), lang_code, "n", "r");
            //insert_into_wt(word_with_spc.c_str(), word_with_spc.length(), lang_code, "n", "r");
            output_sql(word_with_spc, word_with_spc.length(), lang_code, "n", "r");
        }
    }
}

void predict(fasttext::FastText& m, std::string input, char *out_lang_code) {
    int32_t k = 1;
    fasttext::real threshold = 0.0;
    stringstream ioss(input);
    vector<pair<fasttext::real, string>> predictions;
    m.predictLine(ioss, predictions, k, threshold);
    for (const auto& prediction : predictions) {
        // if (prediction.first > .5)
            strcpy(out_lang_code, strrchr(prediction.second.c_str(), '_')+1);
        // else
        //     strcpy(out_lang_code, "en");
        break;
    }
    ///printPredictions(predictions, true, true);
}

/*
wchar_t parseUTF16LE(char *source) {
    wchar_t wc = 0;
    int i = 4;
    while (i--) {
        char c = *source++;
        wc += (c >= '0' && c <= '9' ? c - '0' : (c >= 'a' && c <= 'f' ? c - 'a' : (c >= 'A' && c <= 'F' ? c - 'A' : 0)));
        wc <<= 4;
    }
    return wc;
}

void appendUTF8(string s, wchar_t uni) {
  if (uni < (1 << 11)) {
    s.append(1, 0xC0 + (uni >> 6));
    s.append(1, 0x80 + (uni & 0x3F));
  } else
  if (uni < (1 << 16)) {
    s.append(1, 0xE0 + (uni >> 12));
    s.append(1, 0x80 + ((uni >> 6) & 0x3F));
    s.append(1, 0x80 + (uni & 0x3F));
  } else {
    s.append(1, 0xF0 + (uni >> 18));
    s.append(1, 0x80 + ((uni >> 12) & 0x3F));
    s.append(1, 0x80 + ((uni >> 6) & 0x3F));
    s.append(1, 0x80 + (uni & 0x3F));
  }

}

string parseBody(char *source, int len) {
    const char *body_start = "\"body\":\"";
    while (*source != '\0' && memcmp(source, body_start, 8) != 0)
        source++;
    source += 8;
    string s;
    while (*source != '\0') {
        if (*source == '\\') {
            source++;
            if (*source == 'u') {
                wchar_t wc = parseUTF16LE(++source);
                appendUTF8(s, wc);
                source += 4;
            } else
                s.append(1, *source);
        } else if (*source == '"')
            break;
        else
            s.append(1, *source);
        source++;
    }
    cout << s << endl;
    return s;
}
*/

int32_t transform_ltr(int32_t ltr) {
    if (ltr >= 'A' && ltr <= 'Z')
        return ltr + ('a' - 'A');
    if (ltr < 127)
        return ltr;
    if (ltr < 8212 || ltr > 8288)
        return ltr;
    switch (ltr) {
        case 8212:
            ltr = 45;
            break;
        case 8216:
        case 8217:
            ltr = 39;
            break;
        case 8220:
        case 8221:
        case 8223:
            ltr = 34;
            break;
        case 8230:
        case 8288:
            ltr = 32;
    }
    return ltr;
}

int32_t readUTF8(string in_str, int len, int l, int& utf8len) {
    int32_t ret;
    if ((in_str[l] & 0x80) == 0) {
        ret = in_str[l];
        utf8len = 1;
    } else
    if (l < (len - 1) && (in_str[l] & 0xE0) == 0xC0 && (in_str[l + 1] & 0xC0) == 0x80) {
        utf8len = 2;
        ret = (in_str[l] & 0x1F);
        ret <<= 6;
        ret += (in_str[l + 1] & 0x3F);
        if (ret < 0x80)
            ret = 0;
    } else
    if (l < (len - 2) && (in_str[l] & 0xF0) == 0xE0 && (in_str[l + 1] & 0xC0) == 0x80
            && (in_str[l + 2] & 0xC0) == 0x80) {
        utf8len = 3;
        ret = (in_str[l] & 0x0F);
        ret <<= 6;
        ret += (in_str[l + 1] & 0x3F);
        ret <<= 6;
        ret += (in_str[l + 2] & 0x3F);
        if (ret < 0x0800)
            ret = 0;
    } else
    if (l < (len - 3) && (in_str[l] & 0xF8) == 0xF0 && (in_str[l + 1] & 0xC0) == 0x80
            && (in_str[l + 2] & 0xC0) == 0x80 && (in_str[l + 3] & 0xC0) == 0x80) {
        utf8len = 4;
        ret = (in_str[l] & 0x07);
        ret <<= 6;
        ret += (in_str[l + 1] & 0x3F);
        ret <<= 6;
        ret += (in_str[l + 2] & 0x3F);
        ret <<= 6;
        ret += (in_str[l + 3] & 0x3F);
        if (ret < 0x10000)
            ret = 0;
    }
    return ret;
}

int is_word(int32_t ltr) {
    if ((ltr >= 'a' && ltr <= 'z') || (ltr >= 'A' && ltr <= 'Z'))
        return 1;
    if (ltr > 128 && ltr < 0x1F000 && ltr != 8205)
        return 2;
    if (ltr == '_' || ltr == '\'' || ltr == '-')
        return 3;
    if (ltr >= '0' && ltr <= '9')
        return 4;
    return 0;
}

void insert_grams_in_word(char *lang_code, wstring& word_to_insert, int word_len, int max_ord) {
    int min_gram_len = max_ord > 2047 ? 2 : (max_ord > 126 ? 3 : 5);
    if (word_len <= min_gram_len)
        return;
    for (int ltr_pos = 0; ltr_pos < word_len - min_gram_len + 1; ltr_pos++) {
        for (int gram_len = min_gram_len; gram_len < (ltr_pos == 0 ? word_len : word_len - ltr_pos + 1); gram_len++) {
            num_grams++;
            //if word_to_insert[ltr_pos:gram_len] in ['to','of'] and langw=='en' and min_gram_len == 2:
            //    print(word_to_insert, max_ord, langw)
            wstring gram = word_to_insert.substr(ltr_pos, gram_len);
            insert_data(lang_code, gram, "n", max_ord);
        }
    }
}

#define MAX_WORDS_PER_PHRASE 5
void process_word_buf_rest(wstring& word_buf, int word_buf_count, int max_ord_phrase, char *lang_code, bool is_spaceless_lang) {
    if (word_buf_count < 3)
        return;
    size_t spc_pos = 0;
    for (int n = 1; n < word_buf_count - 1; n++) {
        size_t next_spc_pos = word_buf.find(' ', spc_pos) + 1;
        wstring wstr = (is_spaceless_lang || word_buf[0] > 0x1F000 || word_buf[0] == 8205 ? word_buf.substr(n)
                            : word_buf.substr(next_spc_pos));
        insert_data(lang_code, wstr, "y", max_ord_phrase);
        spc_pos = next_spc_pos;
        num_phrases++;
    }
}

void process_word(char *lang_code, wstring& word, wstring& word_buf, int& word_buf_count, int &max_ord_phrase, int max_ord, bool is_spaceless_lang, bool is_compound) {
    if (word.length() == 0)
        return;
    if (is_spaceless_lang && max_ord < 2048)
        is_spaceless_lang = false;
    if (word[0] == ' ' || word[0] == 8205) {
        if (word_buf_count == MAX_WORDS_PER_PHRASE) {
            if (word.length() == 2 && (is_spaceless_lang || word_buf[1] > 0x1F000 || word_buf[1] == 8205))
                word_buf.erase(0, 1);
            else {
                size_t spc_pos = word_buf.find(' ');
                word_buf.erase(0, spc_pos + 1);
            }
            word_buf_count--;
        }
        if (max_ord > max_ord_phrase)
            max_ord_phrase = max_ord;
        if (word.length() == 2 && (is_spaceless_lang || word[1] > 0x1F000 || word[1] == 8205))
            word_buf.append(word.substr(1));
        else
            word_buf.append(word);
        word_buf_count++;
        insert_data(lang_code, word_buf, "y", max_ord_phrase);
        num_phrases++;
        process_word_buf_rest(word_buf, word_buf_count, max_ord_phrase, lang_code, is_spaceless_lang);
    } else {
        word_buf.assign(word);
        word_buf_count = 1;
        max_ord_phrase = max_ord;
    }
    if (word[0] == ' ')
        word.erase(0, 1);
    if (word.length() == 0 || word[0] == 8205)
        return;
    num_words++;
    insert_data(lang_code, word, "y", max_ord);
    if (!is_compound && (word[0] < '0' || word[0] > '9') && is_word(word[0]) && word.length() < 16 && !is_spaceless_lang)
        insert_grams_in_word(lang_code, word, word.length(), max_ord);
}

const int MAX_WORD_LEN = 21;
void split_words(string& str2split, char *lang, bool is_spaceless_lang) {
    wstring word;
    int max_ord = 0;
    int same_ltr_count = 0;
    int32_t prev_ltr = 0;
    const char *s_cstr = str2split.c_str();
    int utf8len;
    bool is_compound = false;
    wstring word_buf;
    int word_buf_count = 0;
    int max_ord_phrase = 0;
    for (int i = 0; i < str2split.length(); i += utf8len) {
        if (i > 0 && str2split[i] == '/' && (str2split[i-1] == 'r' || str2split[i-1] == 'u')
                && (i == 1 || (i > 1 && (str2split[i-2] == ' ' || str2split[i-2] == '/'
                               || str2split[i-2] == '\n' || str2split[i-2] == '|')))) {
            while (++i < str2split.length() && is_word(str2split[i]));
            prev_ltr = 0;
            max_ord = 0;
            same_ltr_count = 0;
            word.clear();
            utf8len = 1;
            continue;
        }
        int32_t ltr = readUTF8(str2split, str2split.length(), i, utf8len);
        int32_t ltr_t = transform_ltr(ltr);
        int ltr_type = is_word(ltr_t);
        if (ltr_type) {
            if (ltr_t > max_ord)
                max_ord = ltr_t;
            if (prev_ltr >= 0x1F000) {
                process_word(lang, word, word_buf, word_buf_count, max_ord_phrase, max_ord, is_spaceless_lang, is_compound);
                word.clear();
                word_buf.clear();
                word_buf_count = 0;
                max_ord = ltr_t;
            }
            if (is_spaceless_lang && ltr_t > 127) {
                word.clear();
                if (i > 0 && is_word(prev_ltr))
                    word.assign(L" ");
                word.append(1, (wchar_t) ltr_t);
                process_word(lang, word, word_buf, word_buf_count, max_ord_phrase, max_ord, is_spaceless_lang, is_compound);
                //word_arr.push_back(word);
                word.clear();
            } else {
                word.append(1, (wchar_t) ltr_t);
                if (ltr_type == 3)
                    is_compound = true;
                if (word.length() > (word[0] == ' ' ? 3 : 2)) {
                    if (prev_ltr == ltr_t && same_ltr_count > -1)
                        same_ltr_count++;
                    else
                        same_ltr_count = -1;
                }
            }
        } else {
            if (word.length() > 0) {
                if (word.find(L"reddit") == string::npos
                                && word.find(L"moderator") == string::npos 
                                && same_ltr_count < 4) {
                    if (word.length() < MAX_WORD_LEN && prev_ltr != '-') {
                        process_word(lang, word, word_buf, word_buf_count, max_ord_phrase, max_ord, is_spaceless_lang, is_compound);
                        word.clear();
                        if (ltr_t == ' ' && prev_ltr < 0x1F000) {
                            int32_t nxt_ltr_t = 0;
                            if ((i + utf8len) < str2split.length()) {
                                int nxt_utf8len;
                                nxt_ltr_t = transform_ltr(readUTF8(str2split, str2split.length(), i + utf8len, nxt_utf8len));
                            }
                            if (is_word(nxt_ltr_t))
                                word.assign(L" ");
                        }
                    } else
                        word.clear();
                } else
                    word.clear();
            }
            if (ltr_type) {
                word.assign(1, (wchar_t) ltr_t);
                max_ord = ltr_t;
            } else
                max_ord = 0;
            if (ltr_t > 0x1F000 || ltr_t == 8205) {
                if (prev_ltr > 0x1F000 || prev_ltr == 8205)
                    word.append(L" ");
                word.append(1, (wchar_t) ltr_t);
                if (max_ord < ltr_t)
                    max_ord = ltr_t;
                if (prev_ltr == ltr_t && same_ltr_count > -1)
                    same_ltr_count++;
                else
                    same_ltr_count = -1;
            } else
                same_ltr_count = 0;
            is_compound = false;
        }
        prev_ltr = ltr_t;
    }
    if (word.length() > 0) {
        if (word.find(L"reddit") == string::npos
                        && word.find(L"moderator") == string::npos 
                        && same_ltr_count < 4)
            if (word.length() < MAX_WORD_LEN && prev_ltr != '-')
                process_word(lang, word, word_buf, word_buf_count, max_ord_phrase, max_ord, is_spaceless_lang, is_compound);
    }
}

const char *lang_list = "[ja][ko][zh][th][my]";
const int lang_list_size = strlen(lang_list);
void processPost(string& utf8body) {

    if (line_count < start_at)
        return;

    //if (utf8body.find("hey y’all, i’m georgia!") == string::npos)
    //    return;
    //if (strcmp(lang_code, "hi") == 0)

    lines_processed++;

    char lang_code[5];
    strcpy(lang_code, "en");
    predict(ftext, utf8body, lang_code);

    //if (strcmp(lang_code, "en") == 0)
    //    return;

    bool is_spaceless_lang = false;
    char boxed_lang[8];
    boxed_lang[0] = '[';
    strcpy(boxed_lang + 1, lang_code);
    if (memmem(lang_list, lang_list_size, boxed_lang, strlen(boxed_lang)) != NULL)
      is_spaceless_lang = true;

    //if (!is_spaceless_lang)
    //    return;

    //    cout << lang_code << endl;
    // cout << "Body: [" << lang_code << "], [" << utf8body << "]" << endl;

    split_words(utf8body, lang_code, is_spaceless_lang);

        // if (line_count > 100000)
        //     exit(1);


    if (lines_processed % 10000 == 0) {
        if (INSERT_INTO_IDX || INSERT_INTO_SQLITE_BLASTER) {
            cache_stats stats = ix_obj->get_cache_stats();
            cout << line_count << " " << lines_processed << " " << ix_obj->get_max_key_len() << " " << ix_obj->get_num_levels()
                  << " w" << stats.pages_written << " r" << stats.pages_read << " m" << stats.total_cache_misses
                  << " f" << stats.cache_flush_count << " r" << stats.total_cache_req << " p" << stats.last_pages_to_flush << endl
                  << " " << words_generated << "=" << num_words << "+" << num_phrases << "+" << num_grams
                  << " i" << words_inserted << " u" << words_updated1 << " u" << words_updated2 //<< " l" << total_word_lens 
                  << " t" << (double) ((double)((int) (duration<double>(steady_clock::now()-start).count() * 1000)) / 1000) << endl;
            //cout << ix_obj->size() << endl;
        } else {
            cout << line_count << " " << lines_processed << " "
                << words_generated << "=" << num_words << "+" << num_phrases << "+" << num_grams << " "
                << words_inserted << " " << total_word_lens << " " 
                << duration<double>(steady_clock::now()-start).count() << endl;
        }
        start = steady_clock::now();
        if (GEN_SQL) {
            cout << "COMMIT; BEGIN EXCLUSIVE;" << endl;
        }
        if (INSERT_INTO_SQLITE) {
            rc = sqlite3_exec(db, "COMMIT;BEGIN EXCLUSIVE", NULL, NULL, NULL);
            if (rc) {
                fprintf(stderr, "Can't begin txn: %s\n", sqlite3_errmsg(db));
                return;
            }
        }
        if (INSERT_INTO_LMDB) {
            //mdb_cursor_close(cursor);
            rc = mdb_txn_commit(txn);
            if (rc) {
                fprintf(stderr, "mdb_txn_commit: (%d) %s\n", rc, mdb_strerror(rc));
                return;
            }
            mdb_dbi_close(env, dbi);
            rc = mdb_txn_begin(env, NULL, 0, &txn);
            if (rc) {
                fprintf(stderr, "mdb_txn_begin: (%d) %s\n", rc, mdb_strerror(rc));
                return;
            }
            rc = mdb_dbi_open(txn, NULL, 0, &dbi);
            if (rc) {
                fprintf(stderr, "mdb_dbi_open: (%d) %s\n", rc, mdb_strerror(rc));
                return;
            }
            // rc = mdb_cursor_open(txn, dbi, &cursor);
            // if (rc) {
            //     fprintf(stderr, "mdb_cursor_renew: (%d) %s\n", rc, mdb_strerror(rc));
            //     return;
            // }
        }
    }

    //if (utf8body.find("hey y’all, i’m georgia!") != string::npos)
    //    exit(1);

}

void processPost(rapidjson::Document& d) {

    string author = d["author"].IsString() ? d["author"].GetString() : "null";
    string distinguished = d["distinguished"].IsString() ? d["distinguished"].GetString() : "null";
    string utf8body = d["body"].GetString();
    if (utf8body.compare("[deleted]") == 0
        || author.compare("AutoModerator") == 0
        || distinguished.compare("moderator") == 0
        || author.find("bot") != string::npos
        || author.find("Bot") != string::npos
        || utf8body.find("I am a bot") != string::npos
        || utf8body.find("was posted by a bot") != string::npos ) {
                return;
    }
    processPost(utf8body);
}

void processOutput(void* buf, size_t len) {
    char *working_buf = (char *) buf;
    char *lf_pos = (char *) memchr(working_buf, 10, len);
    if (remain_buf.size() > 0 && lf_pos != NULL) {
        remain_buf.append(working_buf, lf_pos - working_buf);
        rapidjson::Document d;
        line_count++;
        if (line_count >= start_at) {
            if (!d.Parse(remain_buf.c_str()).HasParseError()) {
                //cout << d["body"].GetString() << endl;
                //cout << "<<---------------- End of body ----------------->>" << endl;
                processPost(d);
            } else {
                cout << "Parser error:" << endl;
                string s(&remain_buf[0], remain_buf.size());
                cout << s << endl;
                cout << "<<---------------- End ----------------->>" << endl;
            }
        }
        remain_buf.erase(remain_buf.begin(), remain_buf.end());
        len -= (lf_pos - working_buf);
        len--;
        working_buf = lf_pos + 1;
        lf_pos = (char *) memchr(working_buf, 10, len);
    }
    while (lf_pos != NULL) {
        *lf_pos = '\0';
        rapidjson::Document d;
        line_count++;
        if (line_count >= start_at) {
            if (!d.ParseInsitu((char *) working_buf).HasParseError()) {
                //cout << d["body"].GetString() << endl;
                //cout << "<<---------------- End of body ----------------->>" << endl;
                processPost(d);
            } else {
                cout << "Parser error in loop:" << endl;
                string s(working_buf, lf_pos - working_buf);
                cout << s << endl;
                cout << "<<---------------- End ----------------->>" << endl;
            }
        }
        len -= (lf_pos - working_buf);
        len--;
        working_buf = lf_pos + 1;
        lf_pos = (char *) memchr(working_buf, 10, len);
    }
    if (len) {
        remain_buf.append(working_buf, len);
        // string s(&remain_buf[0], remain_buf.size());
        // if (s.find("God damn: How's this") != s.npos) {
        //     cout << s << endl;
        //     cout << "<<---------------- End ----------------->>" << endl;
        // }
    }
}

static void decompressFile_orDie(const char* fname)
{
    FILE* const fin  = fopen_orDie(fname, "rb");
    size_t const buffInSize = ZSTD_DStreamInSize();
    void*  const buffIn  = malloc_orDie(buffInSize + 1);
    FILE* const fout = stdout;
    size_t const buffOutSize = ZSTD_DStreamOutSize();  /* Guarantee to successfully flush at least one complete compressed block in all circumstances. */
    void* buffOut = malloc_orDie(buffOutSize + 1);

    ZSTD_DCtx* const dctx = ZSTD_createDCtx();
    CHECK(dctx != NULL, "ZSTD_createDCtx() failed!");
    ZSTD_DCtx_setParameter(dctx, ZSTD_d_windowLogMax, 31);

    /* This loop assumes that the input file is one or more concatenated zstd
     * streams. This example won't work if there is trailing non-zstd data at
     * the end, but streaming decompression in general handles this case.
     * ZSTD_decompressStream() returns 0 exactly when the frame is completed,
     * and doesn't consume input after the frame.
     */
    size_t const toRead = buffInSize;

    size_t read;
    size_t lastRet = 0;
    int isEmpty = 1;
    while ( (read = fread_orDie(buffIn, toRead, fin)) && !kbhit() ) {
        isEmpty = 0;
        ZSTD_inBuffer input = { buffIn, read, 0 };
        /* Given a valid frame, zstd won't consume the last byte of the frame
         * until it has flushed all of the decompressed data of the frame.
         * Therefore, instead of checking if the return code is 0, we can
         * decompress just check if input.pos < input.size.
         */
        while (input.pos < input.size && !kbhit()) {
            ZSTD_outBuffer output = { buffOut, buffOutSize, 0 };
            /* The return code is zero if the frame is complete, but there may
             * be multiple frames concatenated together. Zstd will automatically
             * reset the context when a frame is complete. Still, calling
             * ZSTD_DCtx_reset() can be useful to reset the context to a clean
             * state, for instance if the last decompression call returned an
             * error.
             */
            size_t const ret = ZSTD_decompressStream(dctx, &output , &input);
            CHECK_ZSTD(ret);
            //fwrite_orDie(buffOut, output.pos, fout);
            processOutput(buffOut, output.pos);
            lastRet = ret;
        }
    }
 
    if (isEmpty) {
        fprintf(stderr, "input is empty\n");
        exit(1);
    }
 
    if (lastRet != 0) {
        /* The last return value from ZSTD_decompressStream did not end on a
         * frame, but we reached the end of the file! We assume this is an
         * error, and the input was truncated.
         */
        fprintf(stderr, "EOF before end of stream: %zu, %zu\n", lastRet, line_count);
        //exit(1);
    }

    ZSTD_freeDCtx(dctx);
    fclose_orDie(fin);
    //fclose_orDie(fout); // don't close stdout
    free(buffIn);
    free(buffOut);
}
 
int main(int argc, const char** argv)
{
    const char* const exeName = argv[0];

        // string s = "We could almost make a thread to make it easier for those who is interested in the actual topic, however I will say sorry for going off the rails here, I sadly tend to get quite caught up when I end up in a discussion. I also have to say sorry for all of the typos and such, I lack a correcting program on my computer and English is not my native language. \n\n\n"
        // "The quote should work quite well as the order was unclear. Edmure is still beneath Robb and does what he is commanded, so he should be looked upon as a soldier even thought he commands the forces of RR.\n\n\n"
        // "Anyways as of the order, it is pretty unclear to me. It does not say anything about not engaging the lannisters. Edmure was never informed  by Robb what the order **hold Riverrun** really meant, and that is a crucial mistake no matter how you look on it. I would have blamed Edmure if the order was **Holder Riverrun only if it is attacked, but otherwise leave Tywin to march through your lands and kill your smallfolks.**\n\n\n"
        // "Robb worked on the assumption that people would understand the plan that he made *after* leaving RR\n\n\n"
        // "It's not exactly obvious either. For all Edmure knows the lannisters still hold the two passes out of the Westerlands, unless you go far to the south of Lannisport. Robb looked like he was trapped there. And Robb only had 6,000 horse. Tywin probably had more, as well as lots of foot, including archers who can be lethal to horsemen without footmen to guard them etc. Tywin still had most of the castles in the west, would be able to raise more men etc. Why should Edmure guess Robb wanted to confront Tywin in the west. It doesn't make much sense before it's explained by the Blackfish. \n\n\n"
        // "Anyways we know that the Tyrells must have been already preparing to march of KL to support it, the question is really if they would have done it even thought Tywin wasn’t with them.  If that is so it would not matter if Tywin marched into the Westerlands.  \n\n\n"
        // "Anyways my question is how you can blame Edmure for doing what he did on such vague orders when he was not informed better? If it is due to blaming him for the RW, then you should remember that he was sacrificing himself to a Frey to attempt to mend the bond that his liege lord managed to shatter,  it is also important to remember that the Young Wolf lost loads of horsemen by fucking over the Freys (and moral of troops) so even if Tywin went into the trap he would outnumber the Young Wolf, and probably be awaiting some sort of engagement seeing as he gets report where the Young Wolf ravage his lands.  There is also the fine opportunity of  Tywin winning, and even then people would blame Edmure. It is important to remember these three things about Edmure: \n\n\n"
        // "1.	We only have Cat`s PoV from the battle, which are second hand and her views on him have been affected by always remembering him as clumsy, naïve and all that shit.\n"
        // "2.	He is to good a person to really be a lord, however he still did as he was ordered.  To *hold Riverrun* , he had not gained any new orders such as letting Tywin march through his lands because Robb or Blackfish did not dare sending a raven or a outrider.\n"
        // "3.	**He cannot read minds and would therefore not know about this “masterplan” that would only give the small northern army some defense advantage, making Tywin bleed in the engagement, but we have no idea if they would have won.**\n\n\n"
        // "Heck, we do not even know if the Blackfish or the Young Wolf really had a plan, it could be something that they made up in an attempt to win back some of the allies that Robb married by his useless political moves. Or else they come off as to drunk on their former glory that they think they would have won the fight no matter how many more men the Lannisters had. \n";

        // s = "Good boy 👨🏻‍🌾🌎💜how are you?";
        // s = "💜how are";
        // //s = "Good 🌎💜";
        // //s = "树倒猢狲散水能载舟";
        // //split_words(s, "zh", true);
        // //s = "Hello, World \"How\" is Lewis' health now-adays";
        // s = "I LOVE YOU GUYS SO MUCHHHHHHHHHH♥ ";
        // split_words(s, "en", false);
        // return 0;

    if (argc<3) {
        fprintf(stderr, "wrong arguments\n");
        fprintf(stderr, "usage:\n");
        fprintf(stderr, "%s FILE\n", exeName);
        return 1;
    }

    ftext.loadModel(string("lid.176.bin"));

    const char* const inFilename = argv[1];
    if (strcmp(inFilename, "-s") == 0) {
        string s = argv[2];
        INSERT_INTO_IDX = 0;
        INSERT_INTO_SQLITE_BLASTER = 0;
        INSERT_INTO_SQLITE = 0;
        INSERT_INTO_ROCKSDB = 0;
        GEN_SQL = 0;
        processPost(s);
        exit(1);
    }
    string log_name(argv[1]);
    log_name += ".log";
    log_fp = fopen(log_name.c_str(), "wb");
    if (log_fp == NULL) {
        perror("Could not create log file: ");
        return 1;
    }
    log_fd = fileno(log_fp);
    const int cache_size = atoi(argv[2]);
    const int page_size = atoi(argv[3]);
    const char* const outFilename = argv[4];
    cout << "Csz: " << cache_size << ", pgsz: " << page_size << endl;
    if (argc > 5) {
       start_at = atoi(argv[5]);
       lines_processed = start_at;
    }

    if (INSERT_INTO_SQLITE) {
        char cmd_str[100];
        sprintf(cmd_str, "PRAGMA page_size = %d", page_size);
        rc = sqlite3_exec(db, cmd_str, NULL, NULL, NULL);
        rc = sqlite3_open(outFilename, &db);
        //rc = sqlite3_open(":memory:", &db);
        if (rc) {
            fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db));
            return 1;
        } else
            fprintf(stderr, "Opened database successfully\n");

        rc = sqlite3_exec(db, "CREATE TABLE word_freq (word, lang, count, is_word, source, mark, primary key (lang, word)) without rowid", NULL, NULL, NULL);
        if (rc) {
            fprintf(stderr, "Can't create table: %s\n", sqlite3_errmsg(db));
            //return 1;
        }

        rc = sqlite3_exec(db, "PRAGMA synchronous = OFF", NULL, NULL, NULL);
        rc = sqlite3_exec(db, "PRAGMA journal_mode = OFF", NULL, NULL, NULL);
        sprintf(cmd_str, "PRAGMA cache_size = %d", cache_size);
        rc = sqlite3_exec(db, cmd_str, NULL, NULL, NULL);
        rc = sqlite3_exec(db, "PRAGMA threads = 2", NULL, NULL, NULL);
        rc = sqlite3_exec(db, "PRAGMA auto_vacuum = 0", NULL, NULL, NULL);
        rc = sqlite3_exec(db, "PRAGMA temp_store = MEMORY", NULL, NULL, NULL);
        rc = sqlite3_exec(db, "PRAGMA locking_mode = EXCLUSIVE", NULL, NULL, NULL);
        rc = sqlite3_exec(db, "BEGIN EXCLUSIVE", NULL, NULL, NULL);
        if (rc) {
            fprintf(stderr, "Can't begin txn: %s\n", sqlite3_errmsg(db));
            return 1;
        }

        const char *ins_sql_str = "INSERT INTO word_freq (lang, word, count, is_word, source) "
                                    "VALUES (?, ?, ?, ?, ?)";
                                    // " ON CONFLICT DO UPDATE SET count = count + 1, "
                                    // "source = iif(instr(source, 'r') = 0, source||'r', source), "
                                    // "is_word = iif(is_word = 'y', 'y', excluded.is_word)";
        rc = sqlite3_prepare_v2(db, ins_sql_str, strlen(ins_sql_str), &ins_word_freq_stmt, &tail);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "Error preparing statement 1: %s\n", sqlite3_errmsg(db));
            sqlite3_close(db);
            return 1;
        }
        const char *sel_sql_str = "SELECT count, is_word, source FROM word_freq "
                                    "WHERE lang = ? and word = ?";
        rc = sqlite3_prepare_v2(db, sel_sql_str, strlen(sel_sql_str), &sel_word_freq_stmt, &tail);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "Error preparing statement 2: %s\n", sqlite3_errmsg(db));
            sqlite3_close(db);
            return 1;
        }
        const char *upd_sql_str = "UPDATE word_freq set count = count + 1, is_word=iif(is_word='y','y',?), "
                                    "source = iif(instr(source, 'r') = 0, source||'r', source) "
                                    "WHERE lang = ? and word = ?";
        rc = sqlite3_prepare_v2(db, upd_sql_str, strlen(upd_sql_str), &upd_word_freq_stmt, &tail);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "Error preparing statement 3: %s\n", sqlite3_errmsg(db));
            sqlite3_close(db);
            return 1;
        }
        const char *del_sql_str = "DELETE FROM word_freq WHERE lang = ? and word = ?";
        rc = sqlite3_prepare_v2(db, del_sql_str, strlen(del_sql_str), &del_word_freq_stmt, &tail);
        if (rc != SQLITE_OK) {
            fprintf(stderr, "Error preparing statement 4: %s\n", sqlite3_errmsg(db));
            sqlite3_close(db);
            return 1;
        }

        cout << "BEGIN EXCLUSIVE;" << endl;
    }

    if (INSERT_INTO_IDX) {
        ix_obj = new sqlite_index_blaster(2, 1, "key, value", "imain", page_size, cache_size, outFilename);
        //ix_obj = new stager(outFilename, cache_size);
    }

    if (INSERT_INTO_SQLITE_BLASTER) {
        ix_obj = new sqlite_index_blaster(5, 2, "lang, word, count, is_word, source", "imain", page_size, cache_size, outFilename);
    }

    if (INSERT_INTO_LMDB) {
        rc = mdb_env_create(&env);
        if (rc) {
            fprintf(stderr, "mdb_env_create: (%d) %s\n", rc, mdb_strerror(rc));
            return 1;
        }
        rc = mdb_env_open(env, "./lmdb_word_freq", MDB_CREATE, 0664);
        if (rc) {
            fprintf(stderr, "mdb_env_open: (%d) %s\n", rc, mdb_strerror(rc));
            return 1;
        }
        rc = mdb_env_set_mapsize(env, 40000000000UL);
        if (rc) {
            fprintf(stderr, "mdb_env_set_mapsize: (%d) %s\n", rc, mdb_strerror(rc));
            return 1;
        }
        rc = mdb_txn_begin(env, NULL, 0, &txn);
        if (rc) {
            fprintf(stderr, "mdb_txn_begin: (%d) %s\n", rc, mdb_strerror(rc));
            return 1;
        }
        rc = mdb_dbi_open(txn, NULL, 0, &dbi);
        if (rc) {
            fprintf(stderr, "mdb_dbi_open: (%d) %s\n", rc, mdb_strerror(rc));
            return 1;
        }
        // rc = mdb_cursor_open(txn, dbi, &cursor);
        // if (rc) {
        //     fprintf(stderr, "mdb_cursor_open: (%d) %s\n", rc, mdb_strerror(rc));
        //     return 1;
        // }
    }

    if (INSERT_INTO_ROCKSDB) {
      rdb_options.IncreaseParallelism();
      rdb_options.OptimizeLevelStyleCompaction();
      //rdb_options.setCompressionType(CompressionType::kNoCompression);
         rocksdb::BlockBasedTableOptions table_options;
         //table_options.block_size = 16384;
         table_options.enable_index_compression = false;
         table_options.block_cache = rocksdb::NewLRUCache(64 * 1024 * 1024LL);
         rdb_options.table_factory.reset(rocksdb::NewBlockBasedTableFactory(table_options));
         rdb_options.compaction_style = rocksdb::kCompactionStyleLevel;
         rdb_options.disable_auto_compactions = true;
         rdb_options.write_buffer_size = 64 * 1024 * 1024LL;
         rdb_options.max_write_buffer_number = 3;
         rdb_options.target_file_size_base = 64 * 1024 * 1024LL;
         rdb_options.max_background_compactions = 4;
         rdb_options.level0_file_num_compaction_trigger = 8;
         rdb_options.level0_slowdown_writes_trigger = -1; // 17;
         rdb_options.level0_stop_writes_trigger = 36;
         rdb_options.num_levels = 4;
         rdb_options.max_bytes_for_level_base = 512 * 1024 * 1024LL;
         rdb_options.max_bytes_for_level_multiplier = 8;
         rdb_options.compression = rocksdb::CompressionType::kNoCompression;

      // create the DB if it's not already present
      rdb_options.create_if_missing = true;
      rdb_options.rate_limiter = shared_ptr<RateLimiter>(NewGenericRateLimiter(2000000000L));
      // open DB
      Status s = DB::Open(rdb_options, kDBPath, &rocksdb1);
      //assert(s.ok());
    }
/*
    if (INSERT_INTO_WT) {
        wiredtiger_open("./wt_word_freq/", NULL, "create,cache_size=320m,extensions=[/usr/local/lib64/libwiredtiger_snappy.so]", &wt_conn);
        wt_conn->open_session(wt_conn, NULL, NULL, &wt_session);
        wt_session->create(wt_session, "table:word_freq", "type=lsm,key_format=S,value_format=I,block_compressor=snappy,lsm=(chunk_size=64m)");
        wt_session->open_cursor(wt_session, "table:word_freq", NULL, NULL, &wt_cursor);
    }
*/
    start = steady_clock::now();
    decompressFile_orDie(inFilename);

    if (INSERT_INTO_SQLITE) {
        rc = sqlite3_exec(db, "COMMIT", NULL, NULL, NULL);
        if (rc) {
            fprintf(stderr, "Can't commit txn: %s\n", sqlite3_errmsg(db));
            return 1;
        }
        sqlite3_finalize(ins_word_freq_stmt);
        sqlite3_finalize(sel_word_freq_stmt);
        sqlite3_finalize(upd_word_freq_stmt);
        sqlite3_finalize(del_word_freq_stmt);
        sqlite3_close(db);
    }

    if (GEN_SQL) {
        cout << "COMMIT;" << endl;
    }
 
    cout << "Total lines processed: " << lines_processed << endl;
    if (INSERT_INTO_IDX || INSERT_INTO_SQLITE_BLASTER) {
        ix_obj->print_stats(ix_obj->size());
        ix_obj->print_num_levels();
        uint8_t val[5];
        int value_len = 0;
        int count;
        bool is_found = ix_obj->get((uint8_t *) "en the ", 7, &value_len, val);
        if (is_found) {
            count = read_uint32(val);
            cout << count << " " << value_len << endl;
        }
        is_found = ix_obj->get((uint8_t *) "en and ", 7, &value_len, val);
        if (is_found) {
            count = read_uint32(val);
            cout << count << " " << value_len << endl;
        }
        cout << "Max word len: " << max_word_len << endl;
        cout << "Total words generated: " << words_generated << endl;
        cout << "Words inserted " << words_inserted << endl;
        cout << "Words updated: " << words_updated1 << endl;

        delete ix_obj;
    }

    if (INSERT_INTO_ROCKSDB) {
        string count_str;
        Status s = rocksdb1->Get(ReadOptions(), "en the ", &count_str);
        if (!s.IsNotFound())
            cout << count_str << endl;
        s = rocksdb1->Get(ReadOptions(), "en and ", &count_str);
        if (!s.IsNotFound())
            cout << count_str << endl;
        delete rocksdb1;
    }

    if (INSERT_INTO_LMDB) {
        //mdb_cursor_close(cursor);
        rc = mdb_txn_commit(txn);
        if (rc) {
            fprintf(stderr, "mdb_txn_commit: (%d) %s\n", rc, mdb_strerror(rc));
            return 1;
        }
        mdb_dbi_close(env, dbi);
        mdb_env_close(env);
    }

/*
    if (INSERT_INTO_WT) {
      wt_cursor->set_key(wt_cursor, "en the ");
      if (wt_cursor->search(wt_cursor) == 0) {
          uint32_t count = 0;
          wt_cursor->get_value(wt_cursor, &count);
            cout << count << endl;
      }
      wt_cursor->set_key(wt_cursor, "en and ");
      if (wt_cursor->search(wt_cursor) == 0) {
          uint32_t count = 0;
          wt_cursor->get_value(wt_cursor, &count);
            cout << count << endl;
      }
        cout << "Max word len: " << max_word_len << endl;
        cout << "Total words generated: " << words_generated << endl;
        cout << "Words inserted " << words_inserted << endl;
        cout << "Words updated: " << words_updated1 << endl;
        wt_cursor->close(wt_cursor);
        wt_conn->close(wt_conn, NULL);
    }
*/

    char cmd[100];
    pid_t pid = getpid();
    sprintf(cmd, "ps -p %d -o oublk -o inblk", pid);
    system(cmd);
#if defined(__APPLE__)
    rusage_info_current rusage;
    if (proc_pid_rusage(pid, RUSAGE_INFO_CURRENT, (void **)&rusage) == 0)
    {
       cout << rusage.ri_diskio_bytesread << endl;
       cout << rusage.ri_diskio_byteswritten << endl;
    }
#else
    sprintf(cmd, "cat /proc/%d/io", pid);
    system(cmd);
    struct rusage ru;
    if (getrusage(RUSAGE_SELF, &ru) == 0)
    {
        cout << ru.ru_inblock << endl;
        cout << ru.ru_oublock << endl;
    }
#endif

    fclose(log_fp);
    return 0;

}
