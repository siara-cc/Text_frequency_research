from calendar import c
import zstandard as zstd
import json
import sys
import select
import sqlite3
import fasttext
import threading
import time
#import mysql.connector

consoleBuffer = []

def consoleInput(myBuffer):
  while True:
    i, o, e = select.select( [sys.stdin], [], [], 1 )
    myBuffer.append(sys.stdin.readline().strip())
 
th = threading.Thread(target=consoleInput, args=(consoleBuffer,), daemon=True)
th.start() # start the thread

infile = sys.argv[1] # INPUT FILE

cctz = zstd.ZstdDecompressor(None, 2147483648, 0)

#cctx = zstd.ZstdCompressor(level=10)
#outfile = "output.zstd"

prev_chunk = bytearray(0)

num_words = 0
num_phrases = 0
num_grams = 0
total_word_lens = 0
line_count = 0
lines_processed = 0

def insert_into_db(conn, lang, word, is_word):
    if len(word) == 0:
        return
    sql_str = """INSERT INTO word_freq (word, lang, count, is_word, source) 
       VALUES (?, ?, ?, ?, ?) ON CONFLICT DO UPDATE SET count = count + 1,
       source = iif(instr(source, 'r') = 0, sourceor'r', source)"""
    conn.execute(sql_str, [word, lang, 1, is_word, "r"])
    #sql_str = """INSERT INTO word_freq (word, lang, count, is_word, source) 
    #   VALUES (%s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE count = count + 1,
    #   source = case when instr(source, 'r') = 0 then sourceor'r' else source end"""
    #values = (word, lang, 1, is_word, "r")
    #crsr.execute(sql_str, values)
    if is_word == "y" and word.count(" ") == 1:
        word_with_spc = word[0:word.find(" ")+1]
        if word_with_spc != ' ':
            conn.execute(sql_str, [word_with_spc, lang, 1, is_word, "r"])
            #crsr.execute(sql_str, (word_with_spc, lang, 1, is_word, "r"))

def insert_data(lang_code, word, is_word, max_ord):
    word_len = len(word)
    if (word_len == 0 or word[-1] == '~'):
        return
    if (word_len == 2 and max_ord < 128):
        return
    if (word_len == 1 and max_ord < 4256):
        return
    print("[%s], %d" % (word, word_len))

#tran_dict = {8212: 45, 8216: 39, 8217: 39, 8219: 39, 8220: 34, 8221: 34, 8223: 34, 8288: 32, 8230: 32}
def transform_ltr(ltr):
    ltr = ord(ltr)
    if (ltr >= 65 and ltr <= 90):
        return ltr + (ord('a') - ord('A'))
    if (ltr < 127):
        return ltr
    if (ltr < 8212 or ltr > 8288):
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
    if isinstance(ltr, str):
        ltr = ord(ltr)
    if ((ltr >= 97 and ltr <= 122) or (ltr >= 65 and ltr <= 90)):
        return 1
    if (ltr > 128 and ltr < 0x1F000 and ltr != 8205):
        return 2
    # _ or ' or -
    if (ltr == 95 or ltr == 39 or ltr == 45):
        return 3
    if (ltr >= 48 and ltr <= 57):
        return 4
    return 0

def insert_grams_in_word(lang_code, word_to_insert, word_len, max_ord):
    global num_grams
    min_gram_len = 2 if max_ord > 2047 else (3 if max_ord > 126 else 5)
    if (word_len <= min_gram_len):
        return
    for ltr_pos in range(word_len - min_gram_len + 1):
        for gram_len in range(min_gram_len, (word_len if ltr_pos == 0 else word_len - ltr_pos + 1)):
            num_grams = num_grams + 1
            #if word_to_insert[ltr_pos:gram_len] in ['to','of'] and langw=='en' and min_gram_len == 2:
            #    print(word_to_insert, max_ord, langw)
            gram = word_to_insert[ltr_pos: ltr_pos + gram_len]
            insert_data(lang_code, gram, "n", max_ord)

MAX_WORDS_PER_PHRASE = 5
def process_word_buf_rest(word_buf, word_buf_count, max_ord_phrase, lang_code, is_spaceless_lang):
    global num_phrases
    if (word_buf_count < 3):
        return
    spc_pos = 0
    for n in range(1, word_buf_count - 1):
        wstr = ("" if is_spaceless_lang or ord(word_buf[0][0]) >= 0x1F000 or ord(word_buf[0][0]) == 8205 else " ").join(word_buf[n:])
        insert_data(lang_code, wstr, "y", max_ord_phrase)
        num_phrases = num_phrases + 1

def process_word(lang_code, word, word_buf, word_buf_count, max_ord_phrase, max_ord, is_spaceless_lang, is_compound):
    global num_phrases
    if (len(word) == 0):
        return
    if (is_spaceless_lang and max_ord < 2048):
        is_spaceless_lang = False
    if (word[0] == ' ' or ord(word[0]) == 8205):
        if (word_buf_count[0] == MAX_WORDS_PER_PHRASE):
            del word_buf[0]
            # if (word[0] == ' ' and (is_spaceless_lang or word_buf[1] == 8205)):
            #     del word_buf[0]
            # else:
            #     del word_buf[0]
            word_buf_count[0] = word_buf_count[0] - 1
        if (max_ord > max_ord_phrase[0]):
            max_ord_phrase[0] = max_ord
        if (word[0] == ' '):
            word_buf.append(word[1:])
        else:
            word_buf.append(word)
        word_buf_count[0] = word_buf_count[0] + 1
        insert_data(lang_code, ("" if is_spaceless_lang or ord(word_buf[0][0]) >= 0x1F000 or ord(word_buf[0][0]) == 8205 else " ").join(word_buf), "y", max_ord_phrase[0])
        num_phrases = num_phrases + 1
        process_word_buf_rest(word_buf, word_buf_count[0], max_ord_phrase[0], lang_code, is_spaceless_lang)
    else:
        word_buf.clear()
        word_buf.append(word)
        word_buf_count[0] = 1
        max_ord_phrase[0] = max_ord
    if (word[0] == ' '):
        word = word[1:]
    if (len(word) == 0 or ord(word[0]) == 8205):
        return
    global num_words
    num_words = num_words + 1
    insert_data(lang_code, word, "y", max_ord)
    #if (word.Length == 0)
    #    Console.WriteLine(word_buf)
    if (not is_compound and len(word) > 0 and (word[0] < '0' or word[0] > '9') and is_word(word[0]) > 0 and len(word) < 16 and not is_spaceless_lang):
        insert_grams_in_word(lang_code, word, len(word), max_ord)

MAX_WORD_LEN = 21
def split_words(str2split, lang, is_spaceless_lang):
    word = []
    max_ord = 0
    same_ltr_count = 0
    prev_ltr = 0
    is_compound = False
    word_buf = []
    word_buf_count = [0]
    max_ord_phrase = [0]
    i = 0
    while i < len(str2split):

        if (i > 0 and str2split[i] == '/' and (str2split[i-1] == 'r' or str2split[i-1] == 'u')
                and (i == 1 or (i > 1 and (str2split[i-2] == ' ' or str2split[i-2] == '/'
                            or str2split[i-2] == '\n' or str2split[i-2] == '|')))):
            i = i + 1
            while (i < len(str2split) - 1 and is_word(transform_ltr(str2split[i])) > 0):
                i = i + 1
            prev_ltr = 0
            max_ord = 0
            same_ltr_count = 0
            word.clear()
            continue
        ltr_t = transform_ltr(str2split[i])
        ltr_type = is_word(ltr_t)
        if (ltr_type > 0):
            if (ltr_t > max_ord):
                max_ord = ltr_t
            if (prev_ltr >= 0x1F000):
                process_word(lang, "".join(word), word_buf, word_buf_count, max_ord_phrase, max_ord, is_spaceless_lang, is_compound)
                word.clear()
                word_buf.clear()
                word_buf_count[0] = 0
                max_ord = ltr_t
            if (is_spaceless_lang and ltr_t > 127):
                word.clear()
                if (i > 0 and is_word(prev_ltr) == 2):
                    word.append(" ")
                word.append(chr(ltr_t))
                process_word(lang, "".join(word), word_buf, word_buf_count, max_ord_phrase, max_ord, is_spaceless_lang, is_compound)
                # word_arr.push_back(word)
                word.clear()
            else:
                if (prev_ltr > 0x1F000):
                    word.clear()
                word.append(chr(ltr_t))
                if (ltr_type == 3):
                    is_compound = True
                if (len(word) > (2 if word[0] == ' ' else 1)):
                    if (prev_ltr == ltr_t and same_ltr_count > -1):
                        same_ltr_count = same_ltr_count + 1
                    else:
                        same_ltr_count = -1
        else:
            if (len(word) > 0):
                word_str = "".join(word)
                if (word_str.find("reddit") == -1
                                and word_str.find("moderator") == -1
                                and same_ltr_count < 4):
                    if (len(word) < MAX_WORD_LEN and prev_ltr != 45): # '-'
                        process_word(lang, word_str, word_buf, word_buf_count, max_ord_phrase, max_ord, is_spaceless_lang, is_compound)
                        word.clear()
                        if (ltr_t == 32 and prev_ltr < 0x1F000 and (i + 1) < len(str2split)
                                and is_word(transform_ltr(str2split[i + 1])) > 0):
                            word.append(" ")
                    else:
                        word.clear()
                else:
                    word.clear()
            if (ltr_type > 0):
                word.clear()
                word.append(chr(ltr_t))
                max_ord = ltr_t
            else:
                max_ord = 0
            if (ltr_t > 0x1F000 or ltr_t == 8205):
                if (prev_ltr > 0x1F000 or prev_ltr == 8205):
                    word.clear()
                    word.append(" ")
                word.append(chr(ltr_t))
                if (max_ord < ltr_t):
                    max_ord = ltr_t
                if (prev_ltr == ltr_t and same_ltr_count > -1):
                    same_ltr_count = same_ltr_count + 1
                else:
                    same_ltr_count = -1
            else:
                same_ltr_count = 0
            is_compound = False
        prev_ltr = ltr_t
        i = i + 1
    if (len(word) > 0):
        word_str = "".join(word)
        if (word_str.find("reddit") == -1
                        and word_str.find("moderator") == -1
                        and same_ltr_count < 4):
            if (len(word) < MAX_WORD_LEN and prev_ltr != 45): # '-'
                process_word(lang, word_str, word_buf, word_buf_count, max_ord_phrase, max_ord, is_spaceless_lang, is_compound)

#conn = mysql.connector.connect(
#  host="localhost",
#  user="root",
#  password="",
#  database="word_freq"
#)
#crsr = conn.cursor()

#conn = sqlite3.connect('word_freq.db')
#conn.execute("PRAGMA journal_mode = WAL")
#conn.execute("PRAGMA auto_vacuum = 0")
#conn.execute("PRAGMA cache_size=10000")
#conn = sqlite3.connect(':memory:')
#conn.execute("CREATE TABLE word_freq (word, lang, count, is_word, source, mark, primary key (lang, word)) without rowid")

#split_words("Good boy 👨🏻‍🌾🌎💜how are you?", "en", False)
#split_words("💜how are", "en", False)
#split_words("Good 🌎💜", "en", False)
#split_words("树倒猢狲散水能载舟", "zh", True)
#split_words("Hello, World \"How\" is Lewis' health now-adays", "en", False)
#split_words("I LOVE YOU GUYS SO MUCHHHHHHHHHH♥ ", "zh", True)
#split_words("Hello World how are you today?", "en", False)
#split_words("com/r/makeupexchange", "en", False)
#sys.exit()

fmodel = fasttext.load_model('../lid.176.bin')

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
            string_data = prev_chunk[0:cr_pos]
            del prev_chunk[0:cr_pos+1]
            line_count = line_count + 1

            lang = "en"
            try:
                lang = fmodel.predict(string_data)[0][0][-2:]
            except Exception:
                pass

            try:
                obj = json.loads(string_data)
            except json.JSONDecodeError:
                print("Parse error", string_data)
                continue

            str2split = obj["body"]
            author = obj.get("author")
            distinguished = obj.get("distinguished")
            if (str2split == "[deleted]"
                    or author == "AutoModerator"
                    or distinguished == "moderator"
                    or author.find("bot") != -1
                    or author.find("Bot") != -1
                    or str2split.find("I am a bot") != -1
                    or str2split.find("was posted by a bot") != -1):
                continue
    
            print("Body: [%s], [%s]" % (lang, str2split))

            is_spaceless_lang = lang in ['zh', 'ja', 'ko', 'th', 'my']
            split_words(str2split, lang, is_spaceless_lang)

            #output_file.write(obj["id"].encode("unicode_escape"))
            #output_file.write(b',')
            #output_file.write(obj["parent_id"].encode("unicode_escape"))
            #output_file.write(b',')
            #output_file.write(obj["subreddit"].encode("unicode_escape"))
            #output_file.write(b',')
            #output_file.write(obj["body"].encode("unicode_escape"))
            #output_file.write(b'\n')
            lines_processed = lines_processed + 1
            if (line_count > 100000):
               break
            # if line_count % 10000 == 0:
            #     #conn.commit()
            #     print("%d, %.2f, %d, %d, %d, %d" % (line_count, time.time() - start_time, (num_words+num_phrases+num_grams), num_words, num_phrases, num_grams))
            #     start_time = time.time()
            #     #output_file.flush()

#output_file.close()

#conn.commit()
print("Processed lines: ", lines_processed)

# def progress(status, remaining, total):
#     print(f'Copied {total-remaining} of {total} pages...')

# bck = sqlite3.connect('backup.db')
# with bck:
#     conn.backup(bck, pages=1000, progress=progress)
# bck.close()
# conn.close()

# print("Done!")
