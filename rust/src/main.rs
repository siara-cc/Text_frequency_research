use std::env;
use std::fs::File;
use std::time::Instant;
use fasttext::{FastText};
use std::io::{Read, BufReader};
use serde_json::{Result as JsonResult, Value};
use zstd::stream::{read::Decoder};

static mut num_words : i64 = 0;
static mut num_phrases : i64 = 0;
static mut num_grams : i64 = 0;
static mut total_word_lens : i64 = 0;
static mut line_count : i64 = 0;
static mut lines_processed : i64 = 0;

fn insert_data(lang_code: &str, word: &str, is_word: &str, max_ord: u32) {
    let word_len = word.chars().count();
    if word_len == 0 || word.chars().last() == Some('~') {
        return;
    }
    if word_len == 2 && max_ord < 128 {
        return;
    }
    if word_len == 1 && max_ord < 4256 {
        return;
    }
    println!("[{}], {}", word, word_len);
}

fn transform_ltr(ltr: char) -> u32 {
    let mut ltr = ltr as u32;
    if ltr >= 65 && ltr <= 90 {
        return ltr + ('a' as u32 - 'A' as u32);
    }
    if ltr < 127 {
        return ltr;
    }
    if ltr < 8212 || ltr > 8288 {
        return ltr;
    }
    match ltr {
        8212 => ltr = 45,
        8216 => ltr = 39,
        8217 => ltr = 39,
        8220 => ltr = 34,
        8221 => ltr = 34,
        8223 => ltr = 34,
        8230 => ltr = 32,
        8288 => ltr = 32,
        _ => (),
    }
    ltr
}

fn is_word(ltr: char) -> u32 {
    let ltr = ltr as u32;
    if (ltr >= 97 && ltr <= 122) || (ltr >= 65 && ltr <= 90) {
        return 1;
    }
    if ltr > 128 && ltr < 0x1F000 && ltr != 8205 {
        return 2;
    }
    // _ or ' or -
    if ltr == 95 || ltr == 39 || ltr == 45 {
        return 3;
    }
    if ltr >= 48 && ltr <= 57 {
        return 4;
    }
    0
}

fn insert_grams_in_word(lang_code: &str, word_to_insert: &str, word_len: usize, max_ord: u32) {
    let min_gram_len = if max_ord > 2047 { 2 } else if max_ord > 126 { 3 } else { 5 };
    if word_len <= min_gram_len {
        return;
    }
    for ltr_pos in 0..word_len - min_gram_len + 1 {
        for gram_len in min_gram_len..=word_len - ltr_pos {
            unsafe { num_grams += 1; }
            let gram = &word_to_insert[ltr_pos..ltr_pos + gram_len];
            insert_data(lang_code, gram, "n", max_ord);
        }
    }
}

fn process_word_buf_rest(word_buf: &mut Vec<String>, word_buf_count: usize, max_ord_phrase: &mut u32, lang_code: &str, is_spaceless_lang: bool) {
    if word_buf_count < 3 {
        return;
    }
    let spc_pos = 0;
    for n in 1..word_buf_count - 1 {
        let mut wstr = " ".to_string();
        if is_spaceless_lang
            || word_buf[0].chars().next().unwrap() as u32 >= 0x1F000
            || word_buf[0].chars().next().unwrap() as u32 == 8205
        {
            wstr = "".to_string();
        }
        wstr.push_str(&word_buf[n..].concat());
        insert_data(lang_code, &wstr, "y", max_ord_phrase);
        unsafe { num_phrases += 1; }
    }
}

fn process_word(lang_code: &str, word: &str, word_buf: &mut Vec<String>, word_buf_count: &mut i32, max_ord_phrase: &mut i32, max_ord: i32, is_spaceless_lang: &mut bool, is_compound: bool) {
    use std::mem;

    if word.is_empty() {
        return;
    }

    if *is_spaceless_lang && max_ord < 2048 {
        *is_spaceless_lang = false;
    }

    if word.starts_with(' ') || word.chars().nth(0).unwrap() == '\u{200B}' {
        if *word_buf_count == MAX_WORDS_PER_PHRASE {
            word_buf.remove(0);
            *word_buf_count -= 1;
            if word.chars().nth(0).unwrap() == ' ' && (*is_spaceless_lang || word_buf[0].chars().nth(0).unwrap() == '\u{200B}') {
                word_buf.remove(0);
            }
            *max_ord_phrase = max_ord;
        }

        if max_ord > *max_ord_phrase {
            *max_ord_phrase = max_ord;
        }

        if word.chars().nth(0).unwrap() == ' ' {
            word_buf.push(word[1..].to_string());
        } else {
            word_buf.push(word.to_string());
        }

        *word_buf_count += 1;

        insert_data(
            lang_code,
            if *is_spaceless_lang || word_buf[0].chars().nth(0).unwrap() >= '\u{1F000}' || word_buf[0].chars().nth(0).unwrap() == '\u{200B}' {
                word_buf.join("")
            } else {
                format!("{}{}", " ", word_buf.join(""))
            },
            "y",
            *max_ord_phrase,
        );

        unsafe { num_phrases += 1; }

        process_word_buf_rest(word_buf, *word_buf_count, *max_ord_phrase, lang_code, is_spaceless_lang);
    } else {
        word_buf.clear();
        word_buf.push(word.to_string());
        *word_buf_count = 1;
        *max_ord_phrase = max_ord;
    }

    let word = if word.starts_with(' ') { &word[1..] } else { word };
    if word.is_empty() || word.chars().nth(0).unwrap() == '\u{200B}' {
        return;
    }

    unsafe { num_words += 1; }

    insert_data(lang_code, word, "y", max_ord);
    if !is_compound && !word.is_empty() && (word.chars().nth(0).unwrap() < '0' || word.chars().nth(0).unwrap() > '9') && is_word(word.chars().nth(0).unwrap()) > 0 && word.len() < 16 && !is_spaceless_lang {
        insert_grams_in_word(lang_code, word, word.len(), max_ord);
    }
}

const MAX_WORD_LEN: usize = 21;

fn split_words(str2split: &str, lang: &str, is_spaceless_lang: bool) {
    let mut word: Vec<char> = Vec::new();
    let mut max_ord = 0;
    let mut same_ltr_count = 0;
    let mut prev_ltr: u32 = 0;
    let mut is_compound = false;
    let mut word_buf: Vec<String> = Vec::new();
    let mut word_buf_count = vec![0];
    let mut max_ord_phrase = 0;
    let mut i = 0;

    while i < str2split.len() {
        if i > 0
            && str2split[i..=i] == "/"
            && (str2split[i - 1..=i - 1] == "r" || str2split[i - 1..=i - 1] == "u")
            && (i == 1
                || (i > 1
                    && (str2split[i - 2..=i - 2] == " "
                        || str2split[i - 2..=i - 2] == "/"
                        || str2split[i - 2..=i - 2] == "\n"
                        || str2split[i - 2..=i - 2] == "|")))
        {
            i += 1;
            while i < str2split.len() - 1 && is_word(transform_ltr(str2split[i..=i])) > 0 {
                i += 1;
            }
            prev_ltr = 0;
            max_ord = 0;
            same_ltr_count = 0;
            word.clear();
            continue;
        }

        let ltr_t = transform_ltr(str2split[i..=i]);
        let ltr_type = is_word(ltr_t);

        if ltr_type > 0 {
            if ltr_t > max_ord {
                max_ord = ltr_t;
            }
            if prev_ltr >= 0x1F000 {
                process_word(
                    lang,
                    &word.iter().collect::<String>(),
                    &mut word_buf,
                    &mut word_buf_count,
                    &mut max_ord_phrase,
                    max_ord,
                    is_spaceless_lang,
                    is_compound,
                );
                word.clear();
                word_buf.clear();
                word_buf_count[0] = 0;
                max_ord = ltr_t;
            }
            if is_spaceless_lang && ltr_t > 127 {
                word.clear();
                if i > 0 && is_word(prev_ltr) == 2 {
                    word.push(' ');
                }
                word.push(std::char::from_u32(ltr_t).unwrap());
                process_word(
                    lang,
                    &word.iter().collect::<String>(),
                    &mut word_buf,
                    &mut word_buf_count,
                    &mut max_ord_phrase,
                    max_ord,
                    is_spaceless_lang,
                    is_compound,
                );
                word.clear();
            } else {
                if prev_ltr > 0x1F000 {
                    word.clear();
                }
                word.push(std::char::from_u32(ltr_t).unwrap());
                if ltr_type == 3 {
                    is_compound = true;
                }
                if word.len() > if word[0] == ' ' { 3 } else { 2 } {
                    if prev_ltr == ltr_t && same_ltr_count > -1 {
                        same_ltr_count += 1;
                    } else {
                        same_ltr_count = -1;
                    }
                }
            }
        } else {
            if (word.len() > 0) {
                let word_str = word.concat();
                if (!word_str.contains("reddit")
                        && !word_str.contains("moderator")
                        && same_ltr_count < 4) {
                    if (word.len() < MAX_WORD_LEN && prev_ltr != 45) { // '-'
                        process_word(lang, word_str, word_buf, word_buf_count, max_ord_phrase, max_ord, is_spaceless_lang, is_compound);
                        word.clear();
                        if (ltr_t == 32 and prev_ltr < 0x1F000 and (i + 1) < len(str2split)
                                and is_word(transform_ltr(str2split[i + 1])) > 0) {
                            word.push(" ");
                        }
                    } else {
                        word.clear();
                    }
                } else {
                    word.clear();
                }
            }
            if (ltr_type > 0) {
                word.clear();
                word.push(ltr_t as char);
                max_ord = ltr_t;
            } else {
                max_ord = 0;
            }
            if (ltr_t > 0x1F000 || ltr_t == 8205) {
                if (prev_ltr > 0x1F000 or prev_ltr == 8205) {
                    word.clear();
                    word.push(' ');
                }
                word.push(ltr_t as char);
                if (max_ord < ltr_t) {
                    max_ord = ltr_t;
                }
                if (prev_ltr == ltr_t && same_ltr_count > -1) {
                    same_ltr_count = same_ltr_count + 1;
                } else {
                    same_ltr_count = -1;
                }
            } else {
                same_ltr_count = 0;
            }
            is_compound = false;
        }
        prev_ltr = ltr_t;
        i = i + 1;
    }
    if (word.len() > 0) {
        word_str = word.concat();
        if (!word_str.contains("reddit")
                && !word_str.contains("moderator")
                && same_ltr_count < 4) {
            if (word.len() < MAX_WORD_LEN and prev_ltr != 45) { // '-'
                process_word(lang, word_str, word_buf, word_buf_count, max_ord_phrase, max_ord, is_spaceless_lang, is_compound);
            }
        }
    }
}

const MAX_WORDS_PER_PHRASE: usize = 5;
fn main() -> Result<(), Box<dyn std::error::Error>> {

    // Get the command line arguments as a vector of strings
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <input file>", args[0]);
        return Ok(());
    }


    // Load the pre-trained FastText model for language identification
    let mut model = FastText::new();
    model.load_model("../lid.176.bin")?;

    let file = File::open(&args[1])?;
    let reader = BufReader::new(file);

    //let buffer = vec![0; 1 << 31];
    let mut decoder = Decoder::with_buffer(reader)?;
    decoder.window_log_max(31);

    // Accumulate the decompressed data in a String until an end-of-line character is found
    let mut buffer = Vec::new();
    let mut chunk = vec![0; 1024];
    let mut line : String;
    let mut start_time = Instant::now();
    loop {
        let bytes_read = decoder.read(&mut chunk)?;
        if bytes_read == 0 {
            break;
        }
        let pos = chunk.iter().position(|&b| b == b'\n');
        match pos {
            Some(p) => {
                buffer.extend(&chunk[..p]);
                line = String::from_utf8(buffer)?;
                buffer = Vec::from(&chunk[p+1..]);
            }
            None => continue,
        }
        unsafe {
            line_count += 1;
        }
        // Parse the accumulated line as JSON
        let json_obj: JsonResult<Value> = serde_json::from_str(&line.to_string());
        if let Ok(json) = json_obj {
            let str2split = json["body"].as_str().unwrap_or_default();
            let author = json["body"].as_str().unwrap_or_default();
            let distinguished = json["body"].as_str().unwrap_or_default();
            
            if str2split == "[deleted]"
                || author == "AutoModerator"
                || distinguished == "moderator"
                || author.contains("bot") || author.contains("Bot")
                || str2split.contains("I am a bot")
                || str2split.contains("was posted by a bot")
            {
                continue;
            }

            // Make the prediction and print the result
            let result = model.predict(str2split, 1, 0.5)?;
            let mut lang = "en";
            if result.len() > 0 {
                lang = &result[0].label[9..];
            }
            //println!("[{}], [{}]", lang, str2split);
            let is_spaceless_lang = ["zh", "ja", "ko", "th", "my"].contains(&lang);
            //split_words(str2split, lang, is_spaceless_lang);
            
            unsafe {
                lines_processed += 1;
                if line_count % 10000 == 0 {
                    println!("{}, {:.2}, {}, {}, {}, {}", line_count, start_time.elapsed().as_secs_f32(), (num_words+num_phrases+num_grams), num_words, num_phrases, num_grams);
                }
                start_time = Instant::now();
            }
        }
    }

    Ok(())
}
