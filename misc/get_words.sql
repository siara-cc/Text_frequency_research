select distinct '"'||lower(replace(replace(word, '_', ' '),'"',''''))||'",' from word_freq where lang in ('en','es', 'pt', 'fr', 'it', 'de') and count > 38 and length(word) > 5 and word not like '%(%' and word >='a' and word <= 'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz' and (is_word='y' or substr(word,length(word))=' ')and lang='en' order by 1;

select distinct '"'||lower(replace(replace(word, '_', ' '),'"',''''))||'",' from word_freq where lang in ('en','es', 'pt', 'fr', 'it', 'de') and count > 39 and word not like '%(%' and word >='a' and word <= 'zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz' and (is_word='y' or substr(word,length(word))=' ')and (length(word) > 5 or (length(word) == 5 and (word like '%h%' or word like '%u%' or word like '%p%' or word like '%m%' or word like '%g%' or word like '%w%' or word like '%f%' or word like '%y%' or word like '%v%' or word like '%k%' or word like '%q%' or word like '%j%' or word like '%x%' or word like '%z%'))) order by 1;

select lang, word, count, (select count from word_freq w2 where w2.lang=w.lang and w2.word = substr(w.word, 1, length(w.word)-1)) as count1 from word_freq w where substr(w.word, length(w.word))=' ' and count > 31 and count1 is not null order by count desc;

[`|~].?[a-z]+.?[`|~]
[`|~][^a-z^~^`]+[`|~]

["{}_<>:\r\n\[\]\\;'	@*&?!\^|]

[,\.01925\-/34678\(\)=+$%#]

- write new blocks together
- find ways to improve write speed
- have usage count in blocks and position in cache
- 
