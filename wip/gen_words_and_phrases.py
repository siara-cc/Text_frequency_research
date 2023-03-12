from calendar import c
import zstandard as zstd
import json
import sys
import sqlite3
import fasttext
import threading
import time
import mysql.connector

consoleBuffer = []

def consoleInput(myBuffer):
  while True:
    myBuffer.append(input())
 
threading.Thread(target=consoleInput, args=(consoleBuffer,), daemon=True).start() # start the thread

path_to_pretrained_model = './lid.176.bin'
fmodel = fasttext.load_model(path_to_pretrained_model)

infile = sys.argv[1] # INPUT FILE

cctz = zstd.ZstdDecompressor(None, 2147483648, 0)

#cctx = zstd.ZstdCompressor(level=10)
#outfile = "output.zstd"

prev_chunk = bytearray(0)
int num_words = 0;
int num_phrases = 0;
int num_grams = 0;
long total_word_lens = 0;
long line_count = 0;
long lines_processed = 0;

def insert_into_db(conn, lang, word, is_word):
    if len(word) == 0:  # ?????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????????
        return
    sql_str = """INSERT INTO word_freq (word, lang, count, is_word, source) 
       VALUES (?, ?, ?, ?, ?) ON CONFLICT DO UPDATE SET count = count + 1,
       source = iif(instr(source, 'r') = 0, source||'r', source)"""
    conn.execute(sql_str, [word, lang, 1, is_word, "r"])
    #sql_str = """INSERT INTO word_freq (word, lang, count, is_word, source) 
    #   VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE count = count + 1,
    #   source = case when instr(source, 'r') = 0 then source||'r' else source end"""
    #values = (word, lang, 1, is_word, "r")
    #crsr.execute(sql_str, values)
    if is_word == "y" and word.count(" ") == 1:
        word_with_spc = word[0:word.find(" ")+1]
        if word_with_spc != ' ':
            conn.execute(sql_str, [word_with_spc, lang, 1, is_word, "r"])
            #crsr.execute(sql_str, (word_with_spc, lang, 1, is_word, "r"))

def transform_ltr(ltr):
    if (ltr >= 65 and ltr <= 90):
        return ltr + (ord('a') - ord('A'))
    if (ltr < 127):
        return ltr
    if (ltr < 8212 || ltr > 8288):
        return ltr
    match ltr:
        case 8212:
            ltr = 45
        case 8216:
            ltr = 39
        case 8217:
            ltr = 39
        case 8220:
            ltr = 34
        case 8221:
            ltr = 34
        case 8223:
            ltr = 34
        case 8230:
            ltr = 32
        case 8288:
            ltr = 32
    return ltr

def is_word(ltr):
    if ((ltr >= 97 and ltr <= 122) or (ltr >= 65 and ltr <= 90)):
        return 1
    if (ltr > 128 and ltr < 0x1F000 and ltr != 8205):
        return 2
    # _ or ' or -
    if (ltr == 95 or ltr == 39 or ltr == 45)
        return 3
    if (ltr >= 48 and ltr <= 57)
        return 4
    return 0

def insert_grams_in_word(lang_code, word_to_insert, word_len, max_ord):
    min_gram_len = 2 if max_ord > 2047 else (3 if max_ord > 126 else 5)
    if (word_len <= min_gram_len):
        return
    for ltr_pos in range(word_len - min_gram_len + 1):
        for (gram_len in range(min_gram_len, (word_len if ltr_pos == 0 else word_len - ltr_pos + 1)):
            num_grams = num_grams + 1
            #if word_to_insert[ltr_pos:gram_len] in ['to','of'] and langw=='en' and min_gram_len == 2:
            #    print(word_to_insert, max_ord, langw)
            string gram = word_to_insert[ltr_pos, ltr_pos + gram_len]
            insert_data(lang_code, gram, "n", max_ord);

const MAX_WORDS_PER_PHRASE = 5
def process_word_buf_rest(word_buf, word_buf_count, max_ord_phrase, lang_code, is_spaceless_lang):
    if (word_buf_count < 3):
        return
    spc_pos = 0;
    for n in range(1, word_buf_count - 1):
        wstr = word_buf[n:].join()
        insert_data(lang_code, wstr, "y", max_ord_phrase);
        num_phrases = num_phrases + 1

def process_word(lang_code, word, word_buf, word_buf_count, ref int max_ord_phrase, int max_ord, bool is_spaceless_lang, bool is_compound) {
    if (word.Length == 0):
        return
    if (is_spaceless_lang and max_ord < 2048):
        is_spaceless_lang = False
    if (word[0] == ' ' or word[0] == 8205):
        if (word_buf_count == MAX_WORDS_PER_PHRASE) {
            if (word[0] == ' ' && (is_spaceless_lang || char.IsSurrogate(word[1]) || word_buf[1] == 8205))
                word_buf.Remove(0, 1);
            else {
                int spc_pos = word_buf.IndexOf(" ");
                word_buf.Remove(0, spc_pos + 1);
            }
            word_buf_count--;
        }
        if (max_ord > max_ord_phrase)
            max_ord_phrase = max_ord;
        if (word[0] == ' ' && (is_spaceless_lang || char.IsSurrogate(word[1]) || word[1] == 8205))
            word_buf.Append(word.ToString(1, word.Length - 1));
        else
            word_buf.Append(word);
        word_buf_count++;
        insert_data(lang_code, word_buf.ToString(), "y", max_ord_phrase);
        num_phrases++;
        process_word_buf_rest(word_buf, word_buf_count, max_ord_phrase, lang_code, is_spaceless_lang);
    } else {
        word_buf.Clear(); word_buf.Append(word);
        word_buf_count = 1;
        max_ord_phrase = max_ord;
    }
    if (word[0] == ' ')
        word.Remove(0, 1);
    if (word.Length == 0 || word[0] == 8205)
        return;
    num_words++;
    insert_data(lang_code, word.ToString(), "y", max_ord);
    //if (word.Length == 0)
    //    Console.WriteLine(word_buf);
    if (!is_compound && word.Length > 0 && (word[0] < '0' || word[0] > '9') && is_word(word[0]) > 0 && word.Length < 16 && !is_spaceless_lang)
        insert_grams_in_word(lang_code, word.ToString(), word.Length, max_ord);
}

const int MAX_WORD_LEN = 21;
void split_words(string in_str, string lang, bool is_spaceless_lang) {
    // Console.Write("String: [");
    // Console.Write(str2split);
    // Console.WriteLine("]");
    var word = new StringBuilder();
    int max_ord = 0;
    int same_ltr_count = 0;
    int prev_ltr = 0;
    bool is_compound = false;
    var word_buf = new StringBuilder();
    int word_buf_count = 0;
    int max_ord_phrase = 0;
    int[] str2split = GetCodePoints(in_str);
    for (int i = 0; i < str2split.Length; i++) {

        if (i > 0 && str2split[i] == '/' && (str2split[i-1] == 'r' || str2split[i-1] == 'u')
                && (i == 1 || (i > 1 && (str2split[i-2] == ' ' || str2split[i-2] == '/'
                            || str2split[i-2] == '\n' || str2split[i-2] == '|')))) {
            while (i < str2split.Length - 1 && is_word(transform_ltr(str2split[++i])) > 0);
            prev_ltr = 0;
            max_ord = 0;
            same_ltr_count = 0;
            word.Clear();
            continue;
        }
        int ltr_t = transform_ltr(str2split[i]);
        int ltr_type = is_word(ltr_t);
        if (ltr_type > 0) {
            if (ltr_t > max_ord)
                max_ord = ltr_t;
            if (prev_ltr >= 0x1F000) {
                process_word(lang, word, word_buf, ref word_buf_count, ref max_ord_phrase, max_ord, is_spaceless_lang, is_compound);
                word.Clear();
                word_buf.Clear();
                word_buf_count = 0;
                max_ord = ltr_t;
            }
            if (is_spaceless_lang && ltr_t > 127) {
                word.Clear();
                if (i > 0 && is_word(prev_ltr) == 2)
                    word.Append(" ");
                word.Append((char)ltr_t);
                process_word(lang, word, word_buf, ref word_buf_count, ref max_ord_phrase, max_ord, is_spaceless_lang, is_compound);
                //word_arr.push_back(word);
                word.Clear();
            } else {
                if (prev_ltr > 0x1F000)
                    word.Clear();
                word.Append((char)ltr_t);
                if (ltr_type == 3)
                    is_compound = true;
                if (word.Length > (word[0] == ' ' ? 2 : 1)) {
                    if (prev_ltr == ltr_t && same_ltr_count > -1)
                        same_ltr_count++;
                    else
                        same_ltr_count = -1;
                }
            }
        } else {
            if (word.Length > 0) {
                if (word.IndexOf("reddit") == -1
                                && word.IndexOf("moderator") == -1
                                && same_ltr_count < 4) {
                    if (word.Length < MAX_WORD_LEN && prev_ltr != '-') {
                        process_word(lang, word, word_buf, ref word_buf_count, ref max_ord_phrase, max_ord, is_spaceless_lang, is_compound);
                        word.Clear();
                        if (ltr_t == ' ' && prev_ltr < 0x1F000 && (i + 1) < str2split.Length
                                && is_word(transform_ltr(str2split[i + 1])) > 0) {
                            word.Clear(); word.Append(" ");
                        }
                    } else
                        word.Clear();
                } else
                    word.Clear();
            }
            if (ltr_type > 0) {
                word.Clear();
                word.Append((char)ltr_t);
                max_ord = ltr_t;
            } else
                max_ord = 0;
            if (ltr_t > 0x1F000 || ltr_t == 8205) {
                if (prev_ltr > 0x1F000 || prev_ltr == 8205) {
                    word.Clear(); word.Append(" ");
                }
                word.Append(char.ConvertFromUtf32(ltr_t));
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
    if (word.Length > 0) {
        if (word.IndexOf("reddit") == -1
                        && word.IndexOf("moderator") == -1
                        && same_ltr_count < 4) {
            if (word.Length < MAX_WORD_LEN && prev_ltr != '-')
                process_word(lang, word, word_buf, ref word_buf_count, ref max_ord_phrase, max_ord, is_spaceless_lang, is_compound);
        }
    }
}

tran_dict = {8216: 39, 8217: 39, 8219: 39, 8220: 34, 8221: 34, 8223: 34, 8288: 32, 8230: 32}

#conn = mysql.connector.connect(
#  host="localhost",
#  user="root",
#  password="",
#  database="word_freq"
#)
#crsr = conn.cursor()

conn = sqlite3.connect('word_freq.db')
#conn.execute("PRAGMA journal_mode = WAL")
conn.execute("PRAGMA auto_vacuum = 0")
conn.execute("PRAGMA cache_size=10000")
#conn = sqlite3.connect(':memory:')
#conn.execute("CREATE TABLE word_freq (word, lang, count, is_word, source, mark, primary key (lang, word)) without rowid")

start_time = time.time()

with open(infile, "rb") as fh:
#  with open(outfile, "wb") as fhout:
  with cctz.stream_reader(fh,131075,True,True) as reader:
#      with cctx.stream_writer(fhout) as output_file:
        while len(consoleBuffer) == 0:
            chunk = reader.read(100)
            if not chunk:
                break
            prev_chunk.extend(chunk)
            cr_pos = prev_chunk.find(10)
            if cr_pos == -1:
                continue
            string_data = prev_chunk[0:cr_pos].decode("utf-8")
            try:
                obj = json.loads(string_data)
            except json.JSONDecodeError:
                print("Parse error", string_data)
                continue
            del prev_chunk[0:cr_pos+1]
            str2split = obj["body"].translate(tran_dict)
            lang = "en"
            try:
                lang = fmodel.predict(str2split)[0][0][-2:]
            except Exception:
                pass
            is_spaceless_lang = lang in ['zh', 'ja', 'ko', 'th', 'my']
            word_arr = split_words(str2split, lang, is_spaceless_lang)

            #output_file.write(obj["id"].encode("unicode_escape"))
            #output_file.write(b',')
            #output_file.write(obj["parent_id"].encode("unicode_escape"))
            #output_file.write(b',')
            #output_file.write(obj["subreddit"].encode("unicode_escape"))
            #output_file.write(b',')
            #output_file.write(obj["body"].encode("unicode_escape"))
            #output_file.write(b'\n')
            line_num = line_num + 1
            if line_num % 10000 == 0:
                conn.commit()
                print(line_num, time.time() - start_time)
                start_time = time.time()
                #output_file.flush()

#output_file.close()

conn.commit()
print("Processed lines: ", line_num)

# def progress(status, remaining, total):
#     print(f'Copied {total-remaining} of {total} pages...')

# bck = sqlite3.connect('backup.db')
# with bck:
#     conn.backup(bck, pages=1000, progress=progress)
# bck.close()
# conn.close()

# print("Done!")

# use the longest phrase when counts are the same
# discard source not in wiktionary ? with low count?
# discard non space word if spaced one is more than 75%
# discard substrings not words which have not much diff

# CREATE TABLE word_freq (word varchar(100), lang varchar(4), count integer, is_word char(1), source varchar(5), mark varchar(10), primary key (lang, word))
