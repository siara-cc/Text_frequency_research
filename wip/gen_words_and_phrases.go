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
line_num = 1

def is_word(ltr):
    if ord(ltr) > 128:
        return True
    if ltr in ['_', '\'']:
        return True
    if (ltr >= 'a' and ltr <= 'z') or (ltr >= 'A' and ltr <= 'Z') or (ltr >= '0' and ltr <= '9'):
        return True
    return False

def split_words(str2split, lang, is_spaceless_lang):
    str2split = str2split.lower()
    word = ""
    word_arr = []
    strlen = len(str2split)
    for i in range(strlen):
        ltr = str2split[i]
        if is_word(ltr):
            if is_spaceless_lang and ord(ltr) > 127:
                word_arr.append((" " if len(word_arr) > 0 else "") + ltr)
                word = ""
            else:
                word += ltr
        else:
            if len(word) > 0:
                if word.find("reddit") > -1 or word.find("moderator") > -1 or (word.count(word[-1]) > len(word)-2 and len(word) > 2):
                    word = (word[1:] if word[0] == ' ' else word) + "~"
                word_arr.append(word)
                if ltr == ' ' and i + 1 < strlen and is_word(str2split[i + 1]):
                    word = " "
                else:
                    word = ""
    return word_arr

def insert_into_db(crsr, lang, word, is_word):
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
            for word_pos in range(len(word_arr)):
                for n in range(1, len(word_arr)-word_pos+1):
                    if n > 5:
                        break
                    word_to_insert = word_arr[word_pos]
                    is_phrase = False
                    for i in range(1, n):
                        if word_arr[word_pos + i][0] == ' ':
                            word_to_insert += word_arr[word_pos + i][(1 if is_spaceless_lang else 0):]
                            is_phrase = True
                        else:
                            word_to_insert = ""
                            break
                    word_to_insert = word_to_insert.strip()
                    word_len = len(word_to_insert)
                    max_ord = 0
                    for ltr in range(len(word_to_insert)):
                        max_ord = max(ord(word_to_insert[ltr]), max_ord)
                    if word_len > 0 and word_to_insert[-1] != '~' and word_len < 21 and \
                            (word_len > 2 or (word_len == 1 and max_ord > 4255) \
                             or (word_len == 2 and max_ord > 127)):
                        langw = "en"
                        try:
                            langw = fmodel.predict(word_to_insert)[0][0][-2:]
                        except Exception:
                            pass
                        if lang == langw:
                            insert_into_db(crsr, lang, word_to_insert, "y")
                            min_gram_len = 2 if max_ord > 2047 else (3 if max_ord > 126 else 5)
                            if not is_phrase and word_len > min_gram_len and word_len < 16 and not is_spaceless_lang:
                                for ltr_pos in range(word_len - min_gram_len + 1):
                                    for gram_len in range(min_gram_len, word_len if ltr_pos == 0 else word_len - ltr_pos + 1):
                                        #if word_to_insert[ltr_pos:gram_len] in ['to','of'] and langw=='en' and min_gram_len == 2:
                                        #    print(word_to_insert, max_ord, langw)
                                        #pass
                                        insert_into_db(crsr, lang, word_to_insert[ltr_pos:gram_len], "n")

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