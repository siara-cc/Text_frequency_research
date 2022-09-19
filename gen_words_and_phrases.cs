using System;
using System.IO;
using System.Text;
using System.Buffers;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Collections;
using System.Runtime.InteropServices;
using ZstdSharp;

class Reddit_Read_ZStd
{

    static int num_words = 0;
    static int num_phrases = 0;
    static int num_grams = 0;
    static long total_word_lens = 0;
    static bool is_word(char ltr)
    {
        if (ltr > 128)
            return true;
        if (ltr == '_' || ltr == '\'')
            return true;
        if ((ltr >= 'a' && ltr <= 'z') || (ltr >= 'A' && ltr <= 'Z') || (ltr >= '0' && ltr <= '9'))
            return true;
        return false;
    }

    static string[] split_words(string str2split, string lang, bool is_spaceless_lang) {
        str2split = str2split.ToLower();
        string word = "";
        var word_arr = new ArrayList();
        for (int i = 0; i < str2split.Length; i++) {
            char ltr = str2split[i];
            if (is_word(ltr)) {
                if (is_spaceless_lang && ltr > 127) {
                    word = "";
                    if (word_arr.Count > 0)
                        word = " ";
                    word += ltr;
                    word_arr.Add(word);
                    word = "";
                } else
                    word += ltr;
            } else {
                if (word.Length > 0) {
                    string s = word;
                    if (word.Contains("reddit") || word.Contains("moderator")) {
                        if (word[0] == ' ')
                            word = word.Substring(1);
                        word += '~';
                    }
                    word_arr.Add(word);
                    if (ltr == ' ' && i + 1 < str2split.Length && is_word(str2split[i + 1]))
                        word = " ";
                    else
                        word = "";
                }
            }
        }
        return (string[]) word_arr.ToArray(word.GetType());
    }

    [DllImport("../FastText_Lang_bindings/CPP/fasttext_predict.so", EntryPoint = "load_model")]
    public extern static void load_model(byte[] filename);

    [DllImport("../FastText_Lang_bindings/CPP/fasttext_predict.so", EntryPoint = "predict")]
    public extern static void predict(byte[] input, byte[] output);

    public static void Main(string[] args)
    {

        load_model(Encoding.UTF8.GetBytes("lid.176.bin"));

        var infile = Environment.GetCommandLineArgs()[1];

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

        var start_time = DateTime.Now;
        byte[] ba = new byte[100];

        using var strm = System.IO.File.OpenRead(infile);
        {
            using var dstream = new DecompressionStream(strm, 131075, false);
            dstream.SetParameter(ZstdSharp.Unsafe.ZSTD_dParameter.ZSTD_d_windowLogMax, 31);
            {
                while (!Console.KeyAvailable)
                {
                    int iRead = dstream.Read(ba, 0, ba.Length);
                    if (iRead == -1)
                        break;
                    int cr_pos = Array.IndexOf<byte>(ba, (byte) 10);
                    if (cr_pos == -1 && cr_pos < iRead)
                    {
                        prev_chunk.Write(ba, 0, iRead);
                        continue;
                    }
                    if (cr_pos > 0)
                        prev_chunk.Write(ba, 0, cr_pos);
                    var string_data = Encoding.UTF8.GetString(prev_chunk.GetBuffer(), 0, (int) prev_chunk.Length);
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
                    var word_arr = split_words(str2split.ToString(), lang, is_spaceless_lang);
                    for (int word_pos = 0; word_pos < word_arr.Length; word_pos++)
                    {
                        for (int n = 1; n < word_arr.Length - word_pos + 1; n++)
                        {
                            if (n > 5)
                                break;
                            var sb = new StringBuilder(word_arr[word_pos]);
                            bool is_phrase = false;
                            for (int i = 1; i < n; i++)
                            {
                                if (word_arr[word_pos + i][0] == ' ')
                                {
                                    sb.Append(is_spaceless_lang ? word_arr[word_pos + i].Substring(1) : word_arr[word_pos + i]);
                                    is_phrase = true;
                                }
                                else
                                {
                                    sb.Clear();
                                    break;
                                }
                            }
                            string word_to_insert = sb.ToString().Trim();
                            int word_len = word_to_insert.Length;
                            int max_ord = 0;
                            for (int ltr = 0; ltr < word_len; ltr++)
                                max_ord = word_to_insert[ltr] > max_ord ? word_to_insert[ltr] : max_ord;
                            if (word_len > 0 && word_to_insert[word_len-1] != '~' && word_len < 21 &&
                                    (word_len > 2 || (word_len == 1 && max_ord > 4255) 
                                    || (word_len == 2 && max_ord > 127)))
                            {
                                //var langw = "en";
                                //try:
                                //    langw = fmodel.predict(word_to_insert)[0][0][-2:]
                                //except Exception:
                                //    pass
                                if (true) // lang == langw:
                                {
                                    // Console.Write(lang);
                                    // Console.Write(" ");
                                    // Console.WriteLine(word_to_insert);
                                    //insert_into_db(crsr, lang, word_to_insert, "y");
                                    num_words++;
                                    if (is_phrase)
                                        num_phrases++;
                                    total_word_lens += word_to_insert.Length;
                                    int min_gram_len = max_ord > 2047 ? 2 : (max_ord > 126 ? 3 : 5);
                                    if (!is_phrase && word_len > min_gram_len &&  word_len < 16 && !is_spaceless_lang)
                                    {
                                        for (int ltr_pos = 0; ltr_pos < word_len - min_gram_len + 1; ltr_pos++)
                                        {
                                            for (int gram_len = min_gram_len; gram_len < (ltr_pos == 0 ? word_len : word_len - ltr_pos + 1); gram_len++)
                                            {
                                                num_grams++;
                                                total_word_lens += word_to_insert.Substring(ltr_pos, gram_len).Length;
                                                // Console.Write(lang);
                                                // Console.Write(" ");
                                                // Console.WriteLine(word_to_insert.Substring(ltr_pos, gram_len));
                                                //#if word_to_insert[ltr_pos:gram_len] in ['to','of'] and langw=='en' and min_gram_len == 2:
                                                //#    print(word_to_insert, max_ord, langw)
                                                //#pass
                                                //insert_into_db(crsr, lang, word_to_insert[ltr_pos:gram_len], "n");
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
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
                        Console.WriteLine("{0}, {1}, {2}, {3}, {4}", 
                            line_num, DateTime.Now - start_time, num_words, num_phrases, num_grams);
                        start_time = DateTime.Now;
                        //#output_file.flush()
                    }
                }
            }
        }
        // #output_file.close()

        //conn.commit();
        Console.WriteLine("Processed lines: ", line_num);

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
