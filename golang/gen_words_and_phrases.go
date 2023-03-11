package main

/*
#cgo CXXFLAGS: -std=c++11
#cgo LDFLAGS: -lfasttext -lstdc++
#include <stdlib.h>
#include "go_fasttext.h"
*/
import "C"
import (
	"fmt"
	"io"
	"log"
	"os"
	"runtime/debug"
	"strings"
	"time"
	"unicode/utf8"
	"unsafe"

	"github.com/klauspost/compress/zstd"
)

var num_words = 0
var num_phrases = 0
var num_grams = 0
var total_word_lens = 0
var line_count = 0
var lines_processed = 0

func insert_data(lang_code string, word string, is_word string, max_ord int) {

	var word_len = len(word)
	if word_len == 0 || word[word_len-1] == '~' {
		return
	}
	if word_len == 2 && max_ord < 128 {
		return
	}
	if word_len == 1 && max_ord < 4256 {
		return
	}

	//fmt.Printf("[%s], %d\n", word, len(word))

}

func transform_ltr(ltr int) int {
	if ltr >= 'A' && ltr <= 'Z' {
		return ltr + ('a' - 'A')
	}
	if ltr < 127 {
		return ltr
	}
	if ltr < 8212 || ltr > 8288 {
		return ltr
	}
	switch ltr {
	case 8212:
		ltr = 45
		break
	case 8216:
	case 8217:
		ltr = 39
		break
	case 8220:
	case 8221:
	case 8223:
		ltr = 34
		break
	case 8230:
	case 8288:
		ltr = 32
		break
	}
	return ltr
}

func is_word(ltr int) int {
	if (ltr >= 'a' && ltr <= 'z') || (ltr >= 'A' && ltr <= 'Z') {
		return 1
	}
	if ltr > 128 && ltr < 0x1F000 && ltr != 8205 {
		return 2
	}
	if ltr == '_' || ltr == '\'' || ltr == '-' {
		return 3
	}
	if ltr >= '0' && ltr <= '9' {
		return 4
	}
	return 0
}

func iif(condition bool, a int, b int) int {
	if condition {
		return a
	}
	return b
}

func iif_s(condition bool, a string, b string) string {
	if condition {
		return a
	}
	return b
}

func insert_grams_in_word(lang_code string, word_to_insert string, word_len int, max_ord int) {
	var min_gram_len = 5
	if max_ord > 2047 {
		min_gram_len = 2
	}
	if max_ord > 126 {
		min_gram_len = 3
	}
	if word_len <= min_gram_len {
		return
	}
	for ltr_pos := 0; ltr_pos < word_len-min_gram_len+1; ltr_pos++ {
		for gram_len := min_gram_len; gram_len < iif(ltr_pos == 0, word_len, word_len-ltr_pos+1); gram_len++ {
			num_grams++
			//if word_to_insert[ltr_pos:gram_len] in ['to','of'] and langw=='en' and min_gram_len == 2:
			//    print(word_to_insert, max_ord, langw)
			var gram = string([]rune(word_to_insert)[ltr_pos : ltr_pos+gram_len])
			insert_data(lang_code, gram, "n", max_ord)
		}
	}
}

const MAX_WORDS_PER_PHRASE = 5

func process_word_buf_rest(word_buf string, word_buf_count int, max_ord_phrase int, lang_code string, is_spaceless_lang bool) {
	if word_buf_count < 3 {
		return
	}
	var spc_pos = 0
	for n := 1; n < word_buf_count-1; n++ {
		var next_spc_pos = strings.Index(word_buf[spc_pos:], " ") + 1
		if next_spc_pos == 0 {
			break
		}
		var wstr = iif_s(is_spaceless_lang || word_buf[0] > 127, string([]rune(word_buf)[n:]), word_buf[next_spc_pos+spc_pos:])
		insert_data(lang_code, wstr, "y", max_ord_phrase)
		spc_pos += next_spc_pos
		num_phrases++
	}
}

func process_word(lang_code string, word *string, word_buf *string, word_buf_count *int, max_ord_phrase *int, max_ord int, is_spaceless_lang bool, is_compound bool) {
	if len(*word) == 0 {
		return
	}
	if is_spaceless_lang && max_ord < 2048 {
		is_spaceless_lang = false
	}
	if (*word)[0] == ' ' {
		if *word_buf_count == MAX_WORDS_PER_PHRASE {
			if (*word)[0] == ' ' && (is_spaceless_lang || (*word)[1] > 127) {
				_, size := utf8.DecodeRuneInString(*word_buf)
				*word_buf = (*word_buf)[size:]
			} else {
				var spc_pos = strings.Index(*word_buf, " ")
				*word_buf = (*word_buf)[spc_pos+1:]
			}
			(*word_buf_count)--
		}
		if max_ord > *max_ord_phrase {
			*max_ord_phrase = max_ord
		}
		if (*word)[0] == ' ' && (is_spaceless_lang || (*word)[1] > 127) {
			*word_buf += (*word)[1:]
		} else {
			*word_buf += *word
		}
		(*word_buf_count)++
		insert_data(lang_code, *word_buf, "y", *max_ord_phrase)
		num_phrases++
		process_word_buf_rest(*word_buf, *word_buf_count, *max_ord_phrase, lang_code, is_spaceless_lang)
	} else {
		*word_buf = *word
		*word_buf_count = 1
		*max_ord_phrase = max_ord
	}
	if (*word)[0] == ' ' {
		*word = (*word)[1:]
	}
	if len(*word) == 0 {
		return
	}
	num_words++
	insert_data(lang_code, *word, "y", max_ord)
	if !is_compound && len(*word) > 0 && ((*word)[0] < '0' || (*word)[0] > '9') && is_word(int([]rune(*word)[0])) > 0 && utf8.RuneCountInString(*word) < 16 && !is_spaceless_lang {
		insert_grams_in_word(lang_code, *word, utf8.RuneCountInString(*word), max_ord)
	}
}

const MAX_WORD_LEN = 21

func split_words(in_str string, lang string, is_spaceless_lang bool) {
	// fmt.Printf("String: [");
	// fmt.Printf(str2split);
	// fmt.Printf("]");
	var word string
	var max_ord = 0
	var same_ltr_count = 0
	var prev_ltr = 0
	var is_compound = false
	var word_buf string
	var word_buf_count = 0
	var max_ord_phrase = 0
	var str2split = []rune(in_str)
	for i := 0; i < len(str2split); i++ {

		if i > 0 && str2split[i] == '/' && (str2split[i-1] == 'r' || str2split[i-1] == 'u') && (i == 1 || (i > 1 && (str2split[i-2] == ' ' || str2split[i-2] == '/' || str2split[i-2] == '\n' || str2split[i-2] == '|'))) {
			i++
			for i < len(str2split) && is_word(transform_ltr(int(str2split[i]))) > 0 {
				i++
			}
			prev_ltr = 0
			max_ord = 0
			same_ltr_count = 0
			word = ""
			continue
		}
		var ltr_t = transform_ltr(int(str2split[i]))
		var ltr_type = is_word(ltr_t)
		if ltr_type > 0 {
			if ltr_t > max_ord {
				max_ord = ltr_t
			}
			if prev_ltr >= 0x1F000 {
				process_word(lang, &word, &word_buf, &word_buf_count, &max_ord_phrase, max_ord, is_spaceless_lang, is_compound)
				word = ""
				word_buf = ""
				word_buf_count = 0
				max_ord = ltr_t
			}
			if is_spaceless_lang && ltr_t > 127 {
				word = ""
				if i > 0 && is_word(prev_ltr) == 2 {
					word = " "
				}
				word += string(ltr_t)
				process_word(lang, &word, &word_buf, &word_buf_count, &max_ord_phrase, max_ord, is_spaceless_lang, is_compound)
				//word_arr.push_back(word);
				word = ""
			} else {
				if prev_ltr > 0x1F000 {
					word = ""
				}
				word += string(ltr_t)
				if ltr_type == 3 {
					is_compound = true
				}
				if len(word) > iif(word[0] == ' ', 2, 1) {
					if prev_ltr == ltr_t && same_ltr_count > -1 {
						same_ltr_count++
					} else {
						same_ltr_count = -1
					}
				}
			}
		} else {
			if len(word) > 0 {
				if strings.Index(word, "reddit") == -1 && strings.Index(word, "moderator") == -1 && same_ltr_count < 4 {
					if len(word) < MAX_WORD_LEN && prev_ltr != '-' {
						process_word(lang, &word, &word_buf, &word_buf_count, &max_ord_phrase, max_ord, is_spaceless_lang, is_compound)
						word = ""
						if ltr_t == ' ' && prev_ltr < 0x1F000 && (i+1) < len(str2split) && is_word(transform_ltr(int(str2split[i+1]))) > 0 {
							word = " "
						}
					} else {
						word = ""
					}
				} else {
					word = ""
				}
			}
			if ltr_type > 0 {
				word = string(ltr_t)
				max_ord = ltr_t
			} else {
				max_ord = 0
			}
			if ltr_t > 0x1F000 || ltr_t == 8205 {
				if prev_ltr > 0x1F000 || prev_ltr == 8205 {
					word = " "
				}
				word += string(ltr_t)
				if max_ord < ltr_t {
					max_ord = ltr_t
				}
				if prev_ltr == ltr_t && same_ltr_count > -1 {
					same_ltr_count++
				} else {
					same_ltr_count = -1
				}
			} else {
				same_ltr_count = 0
			}
			is_compound = false
		}
		prev_ltr = ltr_t
	}
	if len(word) > 0 {
		if strings.Index(word, "reddit") == -1 && strings.Index(word, "moderator") == -1 && same_ltr_count < 4 {
			if len(word) < MAX_WORD_LEN && prev_ltr != '-' {
				process_word(lang, &word, &word_buf, &word_buf_count, &max_ord_phrase, max_ord, is_spaceless_lang, is_compound)
			}
		}
	}
}

func main() {

	model_name := C.CString("lid.176.bin")
	C.load_model(model_name)
	defer C.free(unsafe.Pointer(model_name))

	//var s = "Good boy 👨🏻‍🌾🌎💜"
	//var s = "💜how are"
	//var s = "Good 🌎💜"
	//var s = "树倒猢狲散水能载舟"
	//split_words(s, "zh", true)
	//var s = "Hello, World \"How\" is Lewis' health now-adays"
	//var s = "I LOVE YOU GUYS SO MUCHHHHHHHHHH♥ "
	// var s = "Hello World how are you there?"
	// split_words(s, "en", false)
	// return

	var infile = os.Args[1]
	inputFile, err := os.Open(infile)
	if err != nil {
		log.Fatal(err)
	}
	defer inputFile.Close()
	zstdReader, err := zstd.NewReader(inputFile, zstd.WithDecoderConcurrency(1), zstd.WithDecoderLowmem(false), zstd.WithDecoderMaxWindow(2<<30))
	if err != nil {
		log.Fatal(err)
	}
	defer zstdReader.Close()

	// var prev_chunk = new(bytes.Buffer)
	line_count = 1

	/*        #conn = mysql.connector.connect(
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
	#conn.execute("CREATE TABLE word_freq (word, lang, count, is_word, source, mark, primary key (lang, word)) without rowid")*/

	var start_time = time.Now()
	var buffer []byte = make([]byte, 100)
	// var json_obj map[string]interface{}

	debug.SetMemoryLimit(3 * 1024 * 1024 * 1024)

	for {
		read_count, err := zstdReader.Read(buffer)
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
		if read_count <= 0 {
			break
		}
		// var cr_pos = bytes.IndexByte(buffer, 10)
		// if cr_pos == -1 || cr_pos >= read_count {
		// 	prev_chunk.Write(buffer[0:read_count])
		// 	continue
		// }
		// if cr_pos > 0 {
		// 	prev_chunk.Write(buffer[0:cr_pos])
		// }
		// json.Unmarshal(prev_chunk.Bytes(), &json_obj)
		// prev_chunk.Truncate(0)
		// prev_chunk.Write(buffer[cr_pos+1 : read_count])
		line_count++
		// if json_obj["body"] == nil {
		// 	continue
		// }
		// var body_str string = json_obj["body"].(string)
		// var author string = "null"
		// if json_obj["author"] != nil {
		// 	author = json_obj["author"].(string)
		// }
		// var distinguished string = "null"
		// if json_obj["distinguished"] != nil {
		// 	author = json_obj["distinguished"].(string)
		// }
		// if body_str == "[deleted]" || author == "AutoModerator" || distinguished == "moderator" || strings.Contains(author, "bot") || strings.Contains(author, "Bot") || strings.Contains(body_str, "I am a bot") || strings.Contains(body_str, "was posted by a bot") {
		// 	continue
		// }
		// ptr := C.malloc(C.sizeof_char * 20)
		// defer C.free(unsafe.Pointer(ptr))
		// c_body_str := C.CString(body_str)
		// defer C.free(unsafe.Pointer(c_body_str))
		// C.predict(c_body_str, (*C.char)(ptr))
		// lang := C.GoString((*C.char)(ptr))

		// fmt.Printf("Body: [")
		// fmt.Printf(body_str)
		// fmt.Printf("]\n")

		// var is_spaceless_lang bool = lang == "zh" || lang == "ja" || lang == "ko" || lang == "th" || lang == "my"
		// split_words(body_str, lang, is_spaceless_lang)

		// #output_file.write(obj["id"].encode("unicode_escape"))
		// #output_file.write(b',')
		// #output_file.write(obj["parent_id"].encode("unicode_escape"))
		// #output_file.write(b',')
		// #output_file.write(obj["subreddit"].encode("unicode_escape"))
		// #output_file.write(b',')
		// #output_file.write(obj["body"].encode("unicode_escape"))
		// #output_file.write(b'\n')
		lines_processed++
		// if line_count > 100000 {
		// 	return
		// }
		if line_count%10000 == 0 {
			//conn.commit();
			fmt.Printf("%d, %.2f, %d, %d, %d, %d\n",
				line_count, time.Since(start_time).Seconds(), (num_words + num_phrases + num_grams), num_words, num_phrases, num_grams)
			start_time = time.Now()
			debug.FreeOSMemory()
			//#output_file.flush()
		}
	}
	// #output_file.close()

	//conn.commit();
	/*
	   fmt.Printf("Processed lines: ", lines_processed);

	   return 0;
	*/
	// # def progress(status, remaining, total):
	// #     print(f'Copied {total-remaining} of {total} pages...')

	// # bck = sqlite3.connect('backup.db')
	// # with bck:
	// #     conn.backup(bck, pages=1000, progress=progress)
	// # bck.close()
	// # conn.close()

	// # print("Done!")

	// # use the longest phrase when counts are the same
	// # discard source not in wiktionary ? with low count?
	// # discard non space word if spaced one is more than 75%
	// # discard substrings not words which have not much diff

	// # CREATE TABLE word_freq (word varchar(100), lang varchar(4), count integer, is_word char(1), source varchar(5), mark varchar(10), primary key (lang, word))
}
