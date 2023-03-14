package cc.siara.word_freq;

import java.io.*;
import java.util.*;

import org.json.JSONObject;
import com.github.luben.zstd.ZstdInputStream;
import com.github.jfasttext.JFastText;

class Reddit_Read_ZStd
{

    static long num_words = 0;
    static long num_phrases = 0;
    static long num_grams = 0;
    static long total_word_lens = 0;
    static JFastText jft = new JFastText();

    static long line_count = 0;
    static long lines_processed = 0;

    static void insert_data(String lang_code, String word, String is_word, int max_ord) {

        int word_len = word.length();
        if (word_len == 0 || word.charAt(word_len-1) == '~')
            return;
        if (word_len == 2 && max_ord < 128)
            return;
        if (word_len == 1 && max_ord < 4256)
            return;

        // System.out.print("[");
        // System.out.print(word);
        // System.out.print("], ");
        // System.out.println(word.length());
    }

    static int transform_ltr(int ltr) {
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
                break;
        }
        return ltr;
    }

    static int is_word(int ltr) {
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

    static void insert_grams_in_word(String lang_code, String word_to_insert, int word_len, int max_ord) {
        int min_gram_len = max_ord > 2047 ? 2 : (max_ord > 126 ? 3 : 5);
        if (word_len <= min_gram_len)
            return;
        for (int ltr_pos = 0; ltr_pos < word_len - min_gram_len + 1; ltr_pos++) {
            for (int gram_len = min_gram_len; gram_len < (ltr_pos == 0 ? word_len : word_len - ltr_pos + 1); gram_len++) {
                num_grams++;
                //if word_to_insert[ltr_pos:gram_len] in ['to','of'] and langw=='en' and min_gram_len == 2:
                //    print(word_to_insert, max_ord, langw)
                String gram = word_to_insert.substring(ltr_pos, ltr_pos + gram_len);
                insert_data(lang_code, gram, "n", max_ord);
            }
        }
    }

    final static int MAX_WORDS_PER_PHRASE = 5;
    static void process_word_buf_rest(StringBuilder word_buf, int word_buf_count, int max_ord_phrase, String lang_code, boolean is_spaceless_lang) {
        if (word_buf_count < 3)
            return;
        int spc_pos = 0;
        for (int n = 1; n < word_buf_count - 1; n++) {
            int next_spc_pos = word_buf.indexOf(" ", spc_pos) + 1;
            String wstr = (is_spaceless_lang || Character.isSurrogate(word_buf.charAt(0)) || word_buf.charAt(0) == 8205
                                ? word_buf.substring(n)
                                : word_buf.substring(next_spc_pos));
            insert_data(lang_code, wstr, "y", max_ord_phrase);
            spc_pos = next_spc_pos;
            num_phrases++;
        }
    }

    static void process_word(String lang_code, StringBuilder word, StringBuilder word_buf, int[] word_buf_count, int[] max_ord_phrase, int max_ord, boolean is_spaceless_lang, boolean is_compound) {
        if (word.length() == 0)
            return;
        if (is_spaceless_lang && max_ord < 2048)
            is_spaceless_lang = false;
        if (word.charAt(0) == ' ' || word.charAt(0) == 8205) {
            if (word_buf_count[0] == MAX_WORDS_PER_PHRASE) {
                if (word.charAt(0) == ' ' && (is_spaceless_lang || Character.isSurrogate(word.charAt(1)) || word_buf.charAt(1) == 8205))
                    word_buf.delete(0, 1);
                else {
                    int spc_pos = word_buf.indexOf(" ");
                    word_buf.delete(0, spc_pos + 1);
                }
                word_buf_count[0]--;
            }
            if (max_ord > max_ord_phrase[0])
                max_ord_phrase[0] = max_ord;
            if (word.charAt(0) == ' ' && (is_spaceless_lang || Character.isSurrogate(word.charAt(1)) || word.charAt(1) == 8205))
                word_buf.append(word.substring(1));
            else
                word_buf.append(word);
            word_buf_count[0]++;
            insert_data(lang_code, word_buf.toString(), "y", max_ord_phrase[0]);
            num_phrases++;
            process_word_buf_rest(word_buf, word_buf_count[0], max_ord_phrase[0], lang_code, is_spaceless_lang);
        } else {
            word_buf.setLength(0); word_buf.append(word);
            word_buf_count[0] = 1;
            max_ord_phrase[0] = max_ord;
        }
        if (word.charAt(0) == ' ')
            word.delete(0, 1);
        if (word.length() == 0 || word.charAt(0) == 8205)
            return;
        num_words++;
        insert_data(lang_code, word.toString(), "y", max_ord);
        //if (word.length() == 0)
        //    System.out.println(word_buf);
        if (!is_compound && word.length() > 0 && (word.charAt(0) < '0' || word.charAt(0) > '9') && is_word(word.charAt(0)) > 0 && word.length() < 16 && !is_spaceless_lang)
            insert_grams_in_word(lang_code, word.toString(), word.length(), max_ord);
    }

    public static Integer[] GetCodePoints(String input)
    {
        ArrayList<Integer> cp_lst = new ArrayList<Integer>();
        for (int i = 0; i < input.length(); i += Character.isSurrogate(input.charAt(i)) ? 2 : 1) {
            int codepoint = Character.codePointAt(input, i);
            cp_lst.add(codepoint);
        }
        return (Integer[]) cp_lst.toArray(new Integer[0]);
    }

    final static int MAX_WORD_LEN = 21;
    static void split_words(String in_str, String lang, boolean is_spaceless_lang) {
        // System.out.print("String: [");
        // System.out.print(str2split);
        // System.out.println("]");
        StringBuilder word = new StringBuilder();
        int max_ord = 0;
        int same_ltr_count = 0;
        int prev_ltr = 0;
        boolean is_compound = false;
        StringBuilder word_buf = new StringBuilder();
        int[] word_buf_count = {0}; // pass int by reference
        int[] max_ord_phrase = {0};
        Integer[] str2split = GetCodePoints(in_str);
        for (int i = 0; i < str2split.length; i++) {

            if (i > 0 && str2split[i] == '/' && (str2split[i-1] == 'r' || str2split[i-1] == 'u')
                    && (i == 1 || (i > 1 && (str2split[i-2] == ' ' || str2split[i-2] == '/'
                                || str2split[i-2] == '\n' || str2split[i-2] == '|')))) {
                while (i < str2split.length - 1 && is_word(transform_ltr(str2split[++i])) > 0);
                prev_ltr = 0;
                max_ord = 0;
                same_ltr_count = 0;
                word.setLength(0);
                continue;
            }
            int ltr_t = transform_ltr(str2split[i]);
            int ltr_type = is_word(ltr_t);
            if (ltr_type > 0) {
                if (ltr_t > max_ord)
                    max_ord = ltr_t;
                if (prev_ltr >= 0x1F000) {
                    process_word(lang, word, word_buf, word_buf_count, max_ord_phrase, max_ord, is_spaceless_lang, is_compound);
                    word.setLength(0);
                    word_buf.setLength(0);
                    word_buf_count[0] = 0;
                    max_ord = ltr_t;
                }
                if (is_spaceless_lang && ltr_t > 127) {
                    word.setLength(0);
                    if (i > 0 && is_word(prev_ltr) == 2)
                        word.append(" ");
                    word.appendCodePoint(ltr_t);
                    process_word(lang, word, word_buf, word_buf_count, max_ord_phrase, max_ord, is_spaceless_lang, is_compound);
                    //word_arr.push_back(word);
                    word.setLength(0);
                } else {
                    if (prev_ltr > 0x1F000)
                        word.setLength(0);
                    word.append((char)ltr_t);
                    if (ltr_type == 3)
                        is_compound = true;
                    if (word.length() > (word.charAt(0) == ' ' ? 3 : 2)) {
                        if (prev_ltr == ltr_t && same_ltr_count > -1)
                            same_ltr_count++;
                        else
                            same_ltr_count = -1;
                    }
                }
            } else {
                if (word.length() > 0) {
                    if (word.indexOf("reddit") == -1
                                    && word.indexOf("moderator") == -1
                                    && same_ltr_count < 4) {
                        if (word.length() < MAX_WORD_LEN && prev_ltr != '-') {
                            process_word(lang, word, word_buf, word_buf_count, max_ord_phrase, max_ord, is_spaceless_lang, is_compound);
                            word.setLength(0);
                            if (ltr_t == ' ' && prev_ltr < 0x1F000 && (i + 1) < str2split.length
                                    && is_word(transform_ltr(str2split[i + 1])) > 0) {
                                word.append(" ");
                            }
                        } else
                            word.setLength(0);
                    } else
                        word.setLength(0);
                }
                if (ltr_type > 0) {
                    word.setLength(0);
                    word.appendCodePoint(ltr_t);
                    max_ord = ltr_t;
                } else
                    max_ord = 0;
                if (ltr_t > 0x1F000 || ltr_t == 8205) {
                    if (prev_ltr > 0x1F000 || prev_ltr == 8205) {
                        word.setLength(0); word.append(" ");
                    }
                    word.appendCodePoint(ltr_t);
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
            if (word.indexOf("reddit") == -1
                            && word.indexOf("moderator") == -1
                            && same_ltr_count < 4) {
                if (word.length() < MAX_WORD_LEN && prev_ltr != '-')
                    process_word(lang, word, word_buf, word_buf_count, max_ord_phrase, max_ord, is_spaceless_lang, is_compound);
            }
        }
    }

    static int indexOf(byte[] arr, byte b) {
        for (int i = 0; i < arr.length; i++) {
            if (arr[i] == b)
                return i;
        }
        return -1;
    }

    public static void main(String args[])
    {

        jft.loadModel("../lid.176.bin");

        // JFastText.ProbLabel probLabel = jft.predictProba(text);
        // System.out.printf("\nThe label of '%s' is '%s' with probability %f\n",
        //         text, probLabel.label, Math.exp(probLabel.logProb));

        String infile = args[0];

        //#var cctz = new Decompressor(null, 2147483648, 0);

        ByteArrayOutputStream prev_chunk = new ByteArrayOutputStream();
        line_count = 1;

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

            try (ZstdInputStream zis = new ZstdInputStream(fis)) {

                zis.setLongMax(31);

                while (System.in.available() == 0) {

                    int iRead = zis.read(ba);
                    if (iRead == -1)
                        break;
                    int cr_pos = indexOf(ba, (byte) 10);
                    if (cr_pos == -1 && cr_pos < iRead)
                    {
                        prev_chunk.write(ba, 0, iRead);
                        continue;
                    }
                    if (cr_pos > 0)
                        prev_chunk.write(ba, 0, cr_pos);
                    String string_data = prev_chunk.toString();
                    if (string_data == null)
                        continue;
                    prev_chunk = new ByteArrayOutputStream();
                    prev_chunk.write(ba, cr_pos + 1, iRead - cr_pos - 1);
                    line_count++;
                    JSONObject obj;
                    try {
                        obj = new JSONObject(string_data);
                    } catch (Exception e) {
                        System.out.print("Parse error: ");
                        System.out.println(e.getMessage());
                        continue;
                    }
                    String body_str = obj.getString("body");
                    if (body_str == null)
                        continue;

                    String author = obj.get("author") == null ? "null" : obj.get("author").toString();
                    String distinguished = obj.get("distinguished") == null ? "null" : obj.get("distinguished").toString();
                    if (body_str.equals("[deleted]")
                        || author.equals("AutoModerator")
                        || distinguished.equals("moderator")
                        || author.contains("bot")
                        || author.contains("Bot")
                        || body_str.contains("I am a bot")
                        || body_str.contains("was posted by a bot") ) {
                                continue;
                    }
    
                    String lang = "en";
                    try {
                        JFastText.ProbLabel probLabel = jft.predictProba(body_str);
                        lang = probLabel.label;
                        lang = lang.substring(lang.lastIndexOf('_') + 1);
                    } catch (Exception e)
                    {
                        System.out.println(e.getMessage());
                    }

                    // System.out.print("Body: [");
                    // System.out.print(lang);
                    // System.out.print("], [");
                    // System.out.print(body_str);
                    // System.out.println("]");

                    boolean is_spaceless_lang = lang.equals("zh") || lang.equals("ja") || lang.equals("ko") || lang.equals("th") || lang.equals("my");
                    split_words(body_str, lang, is_spaceless_lang);
                    lines_processed++;

                    // if (line_count > 100000)
                    //     return;

                    if (line_count % 10000 == 0) {
                        //conn.commit();
                        System.out.printf("%d, %.2f, %d, %d, %d, %d%n",
                            line_count, (double) (System.currentTimeMillis() - start_time)/1000, (num_words+num_phrases+num_grams), num_words, num_phrases, num_grams);
                        start_time = System.currentTimeMillis();
                        //#output_file.flush()
                    }
                }
            }
        } catch (FileNotFoundException fnf) {
            System.out.println("File not found");
            return;
        } catch (IOException ioe) {
            System.out.println("IO Exception" + ioe.getMessage());
            return;
        }
        // #output_file.close()

        //conn.commit();
        System.out.printf("Processed lines: %d\n", lines_processed);

        return;

    }
}
