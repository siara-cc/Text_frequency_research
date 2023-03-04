import java.lang.System;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import com.github.luben.zstd.ZstdInputStream;
import com.github.jfasttext.JFastText;

/*
static class SBExtension {
    public static int IndexOf(
        this StringBuilder sb,
        string value,
        int startIndex = 0,
        bool ignoreCase = false)
    {
        int len = value.Length;
        int max = (sb.Length - len) + 1;
        var v1 = (ignoreCase)
            ? value.ToLower() : value;
        var func1 = (ignoreCase)
            ? new Func<char, char, bool>((x, y) => char.ToLower(x) == y)
            : new Func<char, char, bool>((x, y) => x == y);
        for (int i1 = startIndex; i1 < max; ++i1)
            if (func1(sb[i1], v1[0]))
            {
                int i2 = 1;
                while ((i2 < len) && func1(sb[i1 + i2], v1[i2]))
                    ++i2;
                if (i2 == len)
                    return i1;
            }
        return -1;
    }
}
*/
class Reddit_Read_ZStd
{

    static int num_words = 0;
    static int num_phrases = 0;
    static int num_grams = 0;
    static long total_word_lens = 0;
    static JFastText jft = new JFastText();
/*
    static Int32 transform_ltr(Int32 ltr) {
        if (ltr >= 'A' && ltr <= 'Z')
            return ltr + ('a' - 'A');
        if (ltr < 127)
            return ltr;
        if (ltr < 8216 || ltr > 8288)
            return ltr;
        switch (ltr) {
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
                break;
        }
        return ltr;
    }

    /// Reads UTF-8 character from in. Also returns the number of bytes occupied by the UTF-8 character in utf8len
    static Int32 readUTF8(StringBuilder in_str, int len, int l, ref int utf8len) {
        Int32 ret = 0;
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

    static int is_word(Int32 ltr) {
        if ((ltr >= 'a' && ltr <= 'z') || (ltr >= 'A' && ltr <= 'Z'))
            return 1;
        if (ltr > 128)
            return 2;
        if (ltr == '_' || ltr == '\'' || ltr == '-')
            return 3;
        if (ltr >= '0' && ltr <= '9')
            return 4;
        return 0;
    }

    static void insert_grams_in_word(string lang_code, string word_to_insert, int word_len, int max_ord) {
        int min_gram_len = max_ord > 2047 ? 2 : (max_ord > 126 ? 3 : 5);
        if (word_len <= min_gram_len)
            return;
        for (int ltr_pos = 0; ltr_pos < word_len - min_gram_len + 1; ltr_pos++) {
            for (int gram_len = min_gram_len; gram_len < (ltr_pos == 0 ? word_len : word_len - ltr_pos + 1); gram_len++) {
                num_grams++;
                //if word_to_insert[ltr_pos:gram_len] in ['to','of'] and langw=='en' and min_gram_len == 2:
                //    print(word_to_insert, max_ord, langw)
                string gram = word_to_insert.Substring(ltr_pos, gram_len);
                //insert_data(lang_code, gram, "n", max_ord);
            }
        }
    }

    const int MAX_WORDS_PER_PHRASE = 5;
    static void process_word_buf_rest(StringBuilder word_buf, int word_buf_count, int max_ord_phrase, String lang_code, bool is_spaceless_lang) {
        if (word_buf_count < 3)
            return;
        int spc_pos = 0;
        for (int n = 1; n < word_buf_count - 1; n++) {
            int next_spc_pos = word_buf.IndexOf(" ", spc_pos) + 1;
            string wstr = (is_spaceless_lang ? word_buf.ToString(n, word_buf.Length-n)
                                : word_buf.ToString(next_spc_pos, word_buf.Length-next_spc_pos));
            //insert_data(lang_code, wstr, "y", max_ord_phrase);
            spc_pos = next_spc_pos;
            num_phrases++;
        }
    }

    static void process_word(string lang_code, StringBuilder word, StringBuilder word_buf, ref int word_buf_count, ref int max_ord_phrase, int max_ord, bool is_spaceless_lang, bool is_compound) {
        if (is_spaceless_lang && max_ord < 2048)
            is_spaceless_lang = false;
        if (word[0] == ' ') {
            if (word_buf_count == MAX_WORDS_PER_PHRASE) {
                if (is_spaceless_lang && word.Length == 2)
                    word_buf.Remove(0, 1);
                else {
                    int spc_pos = word_buf.IndexOf(" ");
                    word_buf.Remove(0, spc_pos + 1);
                }
                word_buf_count--;
            }
            if (max_ord > max_ord_phrase)
                max_ord_phrase = max_ord;
            word_buf.Append(is_spaceless_lang && word.Length == 2 ? word.ToString(1, word.Length - 1) : word);
            word_buf_count++;
            //insert_data(lang_code, word_buf, "y", max_ord_phrase);
            num_phrases++;
            process_word_buf_rest(word_buf, word_buf_count, max_ord_phrase, lang_code, is_spaceless_lang);
        } else {
            word_buf.Clear(); word_buf.Append(word);
            word_buf_count = 1;
            max_ord_phrase = max_ord;
        }
        if (word[0] == ' ')
            word.Remove(0, 1);
        num_words++;
        //insert_data(lang_code, word, "y", max_ord);
        if (!is_compound && word.Length > 0 && (word[0] < '0' || word[0] > '9') && is_word(word[0]) > 0 && word.Length < 16 && !is_spaceless_lang)
            insert_grams_in_word(lang_code, word.ToString(), word.Length, max_ord);
    }

    const int MAX_WORD_LEN = 21;
    static void split_words(StringBuilder str2split, string lang, bool is_spaceless_lang) {
        // Console.Write("String: [");
        // Console.Write(str2split);
        // Console.WriteLine("]");
        var word = new StringBuilder();
        int max_ord = 0;
        int same_ltr_count = 0;
        Int32 prev_ltr = 0;
        bool is_compound = false;
        var word_buf = new StringBuilder();
        int word_buf_count = 0;
        int max_ord_phrase = 0;
        for (int i = 0; i < str2split.Length; i++) {
            if (i > 0 && str2split[i] == '/' && (str2split[i-1] == 'r' || str2split[i-1] == 'u')
                    && (i == 1 || (i > 1 && (str2split[i-2] == ' ' || str2split[i-2] == '/'
                                || str2split[i-2] == '\n' || str2split[i-2] == '|')))) {
                while (i < str2split.Length - 1 && is_word(str2split[++i]) > 0);
                prev_ltr = 0;
                max_ord = 0;
                same_ltr_count = 0;
                word.Clear();
                continue;
            }
            Int32 ltr_t = str2split[i];
            if (prev_ltr == 0)
                prev_ltr = ltr_t;
            int ltr_type = is_word(ltr_t);
            // Truth Table for XOR
            // 0 0 - 0 - if ltr_t is emoji and prev_ltr is not emoji
            // 0 1 - 1 - if ltr_t is emoji and prev_ltr is emoji
            // 1 0 - 1 - if ltr_t is not emoji and prev_ltr is not emoji
            // 1 1 - 0 - if ltr_t is not emoji and prev_ltr is emoji
            if (ltr_type > 0 && ((ltr_t < 0x1F000) ^ (prev_ltr >= 0x1F000))) {
                if (ltr_t > max_ord)
                    max_ord = ltr_t;
                if (is_spaceless_lang && ltr_t > 127) {
                    word.Clear();
                    if (i > 0 && is_word(prev_ltr) > 0) {
                        word.Clear(); word.Append(" ");
                    }
                    word.Append((char) ltr_t);
                    process_word(lang, word, word_buf, ref word_buf_count, ref max_ord_phrase, max_ord, is_spaceless_lang, is_compound);
                    //word_arr.push_back(word);
                    word.Clear();
                } else {
                    word.Append((char) ltr_t);
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
                                    && is_word(str2split[i + 1]) > 0) {
                                word.Clear(); word.Append(" ");
                            }
                        } else
                            word.Clear();
                    } else
                        word.Clear();
                }
                if (ltr_t > 0x1F000 || ltr_type > 0) {
                    word.Clear(); word.Append((char) ltr_t);
                    max_ord = ltr_t;
                } else
                    max_ord = 0;
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

*/

    static int indexOf(byte[] arr, byte b) {
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == b)
                return i;
        }
        return -1;
    }

    public static int main(String[] args)
    {

        jft.loadModel("lid.176.bin");

        // JFastText.ProbLabel probLabel = jft.predictProba(text);
        // System.out.printf("\nThe label of '%s' is '%s' with probability %f\n",
        //         text, probLabel.label, Math.exp(probLabel.logProb));

        var infile = args[1];

        //#var cctz = new Decompressor(null, 2147483648, 0);

        var prev_chunk = new MemoryStream();
        var line_num = 1;

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

        long start_time = System.currentTimeMillis();
        byte[] ba = new byte[100];

        try (FileInputStream fis = new FileInputStream(infile)) {

            try (ZstdInputStream zis = new ZstdInputStream(fis, 2 * 1024 * 1024 * 1024)) {

                while (System.in.available() == 0) {

                    int iRead = dstream.read(ba);
                    if (iRead == -1)
                        break;
                    int cr_pos = indexOf(ba, (byte) 10);
                    if (cr_pos == -1 && cr_pos < iRead)
                    {
                        prev_chunk.Write(ba, 0, iRead);
                        continue;
                    }
                    if (cr_pos > 0)
                        prev_chunk.Write(ba, 0, cr_pos);
                    var string_data = Encoding.UTF8.GetString(prev_chunk.GetBuffer(), 0, (int) prev_chunk.Length);
                    if (string_data == null)
                        continue;
                    prev_chunk = new MemoryStream();
                    prev_chunk.Write(ba, cr_pos + 1, iRead - cr_pos - 1);
                    JsonNode obj;
                    try {
                        obj = JsonNode.Parse(string_data);
                    } catch (Exception e) {
                        Console.Write("Parse error: ");
                        Console.WriteLine(e.Message);
                        continue;
                    }
                    if (obj["body"] == null)
                        continue;
                    var str2split = new StringBuilder(obj["body"].ToString());
                    for (int i = 0; i < str2split.Length; i++) {
                        switch ((int)str2split[i]) {
                            case 8216:
                            case 8217:
                                str2split[i] = (char) 39;
                                break;
                            case 8220:
                            case 8221:
                            case 8223:
                                str2split[i] = (char) 34;
                                break;
                            case 8230:
                            case 8288:
                                str2split[i] = (char) 32;
                            break;
                        }
                    }
                    var lang = "en";
                    try {
                        byte[] output = new byte[30];
                        predict(Encoding.UTF8.GetBytes(str2split.ToString()), output);
                        lang = Encoding.UTF8.GetString(output, 0, Array.IndexOf<byte>(output, (byte) '\0'));
                        lang = lang.Substring(lang.LastIndexOf('_') + 1);
                    } catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }
                    bool is_spaceless_lang = lang.Equals("zh") || lang.Equals("ja") || lang.Equals("ko") || lang.Equals("th") || lang.Equals("my");
                    split_words(str2split, lang, is_spaceless_lang);
                    // #output_file.write(obj["id"].encode("unicode_escape"))
                    // #output_file.write(b',')
                    // #output_file.write(obj["parent_id"].encode("unicode_escape"))
                    // #output_file.write(b',')
                    // #output_file.write(obj["subreddit"].encode("unicode_escape"))
                    // #output_file.write(b',')
                    // #output_file.write(obj["body"].encode("unicode_escape"))
                    // #output_file.write(b'\n')
                    line_num = line_num + 1;
                    if (line_num % 10000 == 0)
                    {
                        //conn.commit();
                        Console.WriteLine("{0}, {1}, {2}, {3}, {4}, {5}", 
                            line_num, (DateTime.Now - start_time).Seconds, (num_words+num_phrases+num_grams), num_words, num_phrases, num_grams);
                        start_time = DateTime.Now;
                        //#output_file.flush()
                    }
                }
            }
        }
        // #output_file.close()

        //conn.commit();
        Console.WriteLine("Processed lines: ", line_num);

        return 0;

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
}
