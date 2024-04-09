create table top_words (word varchar primary key, lang varchar, ct integer, lvl integer) without rowid

alter table top_words add lvl integer
update top_words set lvl=0

insert into top_words
select substr(key, 4), substr(key, 1, 2), ct, 0
from mdx
where substr(key, 1, 3) = 'en '
and length(key) > 6
order by ct desc limit 2000000;

update top_words set lvl=1 where word = 'and'
update top_words set lvl=1 where word = 'but'
update top_words set lvl=1 where word = 'for'
update top_words set lvl=1 where word = 'der'
update top_words set lvl=1 where word = 'ing'
update top_words set lvl=1 where word = 'the'
update top_words set lvl=1 where word = 'you'
update top_words set lvl=1 where word = 'que'

-- update top_words set lvl=1 where word = 'des'
-- update top_words set lvl=1 where word = 'has'
-- update top_words set lvl=1 where word = 'how'
-- update top_words set lvl=1 where word = 'was'
-- update top_words set lvl=1 where word = 'not'

-- update top_words set lvl=1 where word = 'that'
-- update top_words set lvl=1 where word = 'ther'
-- update top_words set lvl=1 where word = 'this'
-- update top_words set lvl=1 where word = 'tion'

delete from top_words where word = 'the ';
delete from top_words where word = 'and ';
delete from top_words where word = 'ing ';
delete from top_words where word = 'der ';
delete from top_words where word = 'des ';
delete from top_words where word = 'die ';
delete from top_words where word = 'for ';
delete from top_words where word = 'has ';
delete from top_words where word = 'les ';
delete from top_words where word = 'que ';
delete from top_words where word = 'that';
delete from top_words where word = 'this';
delete from top_words where word = 'was ';
delete from top_words where word = 'ment';

insert into top_words (word, ct, lvl, lang) values ('and ', 0, 1, 'en');
insert into top_words (word, ct, lvl, lang) values ('but ', 0, 1, 'en');
insert into top_words (word, ct, lvl, lang) values ('der ', 0, 1, 'de');
insert into top_words (word, ct, lvl, lang) values ('des ', 0, 1, 'de');
insert into top_words (word, ct, lvl, lang) values ('for ', 0, 1, 'en');
insert into top_words (word, ct, lvl, lang) values ('has ', 0, 1, 'en');
insert into top_words (word, ct, lvl, lang) values ('ing ', 0, 1, 'en');
insert into top_words (word, ct, lvl, lang) values ('les ', 0, 1, 'fr');
insert into top_words (word, ct, lvl, lang) values ('ment', 0, 1, 'en');
insert into top_words (word, ct, lvl, lang) values ('que ', 0, 1, 'es');
insert into top_words (word, ct, lvl, lang) values ('that', 0, 1, 'en');
insert into top_words (word, ct, lvl, lang) values ('the ', 0, 1, 'en');
insert into top_words (word, ct, lvl, lang) values ('this', 0, 1, 'en');
insert into top_words (word, ct, lvl, lang) values ('tion', 0, 1, 'en');
insert into top_words (word, ct, lvl, lang) values ('was ', 0, 1, 'en');
insert into top_words (word, ct, lvl, lang) values ('you ', 0, 1, 'de');

insert into top_words (word, ct, lvl, lang) values ('": "', 0, 2, 'en');
insert into top_words (word, ct, lvl, lang) values ('http://', 0, 2, 'en');
insert into top_words (word, ct, lvl, lang) values ('https://', 0, 2, 'en');
insert into top_words (word, ct, lvl, lang) values ('.com', 0, 2, 'en');
insert into top_words (word, ct, lvl, lang) values ('.org', 0, 2, 'en');
insert into top_words (word, ct, lvl, lang) values ('www.', 0, 2, 'en');
insert into top_words (word, ct, lvl, lang) values ('😍', 0, 2, 'em');
insert into top_words (word, ct, lvl, lang) values ('😭', 0, 2, 'em');
insert into top_words (word, ct, lvl, lang) values ('😐', 0, 2, 'em');
insert into top_words (word, ct, lvl, lang) values ('😐😐', 0, 2, 'em');
insert into top_words (word, ct, lvl, lang) values ('🤣', 0, 2, 'em');
insert into top_words (word, ct, lvl, lang) values ('😂', 0, 2, 'em');
insert into top_words (word, ct, lvl, lang) values ('🔥', 0, 2, 'em');
insert into top_words (word, ct, lvl, lang) values ('🤤', 0, 2, 'em');
insert into top_words (word, ct, lvl, lang) values ('😘', 0, 2, 'em');
insert into top_words (word, ct, lvl, lang) values ('🥰', 0, 2, 'em');


delete from top_words where word='die ';
update top_words set lvl=1 where word='you ';

select word, ct, (select ct from top_words t where t.word = substr(top_words.word, 1, length(top_words.word) - 1)
 and abs(t.ct - top_words.ct) > top_words.ct/2) ct1
from top_words where substr(word, length(word) - 1, 1) = ' ' and ct1 is not null limit 10

delete from top_words where substr(word, length(word) - 1, 1) <> ' ' 
and exists (select ct from top_words t 
where t.word = top_words.word || ' ' and abs(t.ct - top_words.ct) < top_words.ct/2)

delete from top_words where substr(word, length(word) - 1, 1) = ' ' 
and exists (select ct from top_words t where t.word = substr(top_words.word, 1, length(top_words.word) - 1)
 and abs(t.ct - top_words.ct) > top_words.ct/2)

delete from top_words where length(cast(word as blob))=4
and substr(word, 1, 1) in (' ', 'e', 't', 'a', 'o', 'i', 'n', 's', 'r', 'l', 'c', 'd', 'u')
and substr(word, 2, 1) in (' ', 'e', 't', 'a', 'o', 'i', 'n', 's', 'r', 'l', 'c', 'd', 'u')
and substr(word, 3, 1) in (' ', 'e', 't', 'a', 'o', 'i', 'n', 's', 'r', 'l', 'c', 'd', 'u')
and substr(word, 4, 1) in (' ', 'e', 't', 'a', 'o', 'i', 'n', 's', 'r', 'l', 'c', 'd', 'u');

delete from top_words
where substr(word, 2, 1) = ' '
and substr(word, 4, 1) = ' ';

select * from top_words order by ct desc;

en, es, fr, de, pt, it, tr, hi, bn, gu, or, ta, te, kn, ml, si, th, my
ar, ur, fa, zh, ja, ko
ru, uk

insert or ignore into top_words
select substr(key, 4), substr(key, 1, 2), ct, 0
from mdx.mdx
where substr(key, 1, 3) = 'en '
and length(key) > 6
order by ct desc limit 1000000

insert or ignore into top_words
select substr(key, 4), substr(key, 1, 2), ct, 0
from mdx
where substr(key, 1, 3) = 'em ' and key > 'em z'
and length(cast(key as blob)) > 11
order by ct desc limit 15;

insert or ignore into top_words
select substr(key, 4), substr(key, 1, 2), ct, 0
from mdx
where substr(key, 1, 3) = 'zh ' and key > 'zh z'
and unicode(substr(key, 4, 1)) > 12000
and unicode(substr(key, 4, 1)) < 917504
and unicode(substr(key, 4, 1)) <> 12288
and unicode(substr(key, 4, 1)) <> 12644
and unicode(substr(key, 4, 1)) <> 65039
and not unicode(substr(key, 4, 1)) between 119808 and 120837
and instr(key, '。') = 0 and instr(key, '，') = 0
and instr(key, '、') = 0
and length(cast(key as blob)) > 11
order by ct desc limit 100000;

insert or ignore into top_words

select substr(key, 4), substr(key, 1, 2), ct, 0
from mdx
where substr(key, 1, 3) = 'ja ' and key > 'ja z'
and length(cast(key as blob)) > 11
order by ct desc limit 50000;


delete from top_words where length(word) = 3 and lvl = 0


delete from top_words where substr(word,1,1)=' ';
delete from top_words where substr(word,1,1)='''';
delete from top_words where substr(word,1,1)='-';
delete from top_words where substr(word,1,1)='_';
delete from top_words where substr(word,1,1)='0';
delete from top_words where word < '10 lbs';

delete from top_words where substr(word, 1, 1) = ' '  and length(cast(word as blob)) = 3

update top_words set lvl=2 where length(cast(word as blob)) = 4 and lvl=0 and ct > 369798;
update top_words set lvl=3 where length(cast(word as blob)) = 4 and lvl=0 and ct > 10901;
update top_words set lvl=4 where length(cast(word as blob)) = 4 and lvl=0 and ct > 231;
delete from top_words where lvl=0 and length(cast(word as blob))=4; 

update top_words set lvl=2 where word in (select word from top_words t
 where lang='en' or length(cast(word as blob))=4 order by ct desc limit 96);

update top_words set lvl=3 where word in (select word from top_words t
 where lvl=0 order by ct desc limit 768);

update top_words set lvl=4 where word in (select word from top_words t
 where lvl=0 order by ct desc limit 12288);

update top_words set lvl=5 where word in (select word from top_words t
 where lvl=0 order by ct desc limit 262144);

update top_words set lvl=6 where lvl=0;

select count(*) from enw where instr(title, '_') = 0 
and instr(title, '''') = 0 and instr(title, '(') = 0 
and instr(title, ')') = 0 and instr(title, '!') = 0

-- max(length(cast(title as blob)))
select title from enw where instr(title, '_') = 0 
and instr(title, '''') = 0 and instr(title, '(') = 0 
and instr(title, ')') = 0 and instr(title, '!') = 0
and instr(title, '*') = 0 and instr(title, '+') = 0
and instr(title, '/') = 0 and instr(title, ',') = 0 
and instr(title, '.') = 0
and title >= 'A' and substr(title, 2, 1) >= 'a'
and substr(title, 1, 1) <> '-' and substr(title, 2, 1) <> '-'
and length(cast(title as blob)) between 8 and 20
and length(title) > 3