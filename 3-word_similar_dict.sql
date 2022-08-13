use jkgj_log;
create table cy_similar_child_20161206
(word string,
 similar string,
 prop  double)
row format delimited fields terminated by ' ' stored as textfile;

load data local inpath '/home/rd/python/output/word_similar_20161206.txt' overwrite into table jkgj_log.cy_similar_child_20161206 ;

drop table child_word_similar_dict;
create table child_word_similar_dict as
select * from cy_similar_child_20161206
where prop is not null 
and word not rlike '^\\d' 
and word not rlike '^\\w'
and word <>''
and similar not rlike '^\\d'
and similar not rlike '^\\w'
and similar<>'';


drop table child_key_word;
create table child_key_word as
select distinct word
from (
select word from child_word_similar_dict
union all
select similar as word from child_word_similar_dict)
t;



create table tag_similar_20161209
(word string,
 similar string,
 prop  double)
row format delimited fields terminated by ' ' stored as textfile;

load data local inpath '/home/rd/cy/python/output/word_similar_20161209.txt' overwrite into table jkgj_log.tag_similar_20161209 ;



drop table tag_similar_dict;
create table tag_similar_dict as
select * from tag_similar_20161209
where prop is not null 
and word not rlike '^\\d' 
and word not rlike '^\\w'
and word <>''
and word is not null
and word <>' '
and similar not rlike '^\\d'
and similar not rlike '^\\w'
and similar<>''
and similar is not null;


drop table tag_similar_dict_unique_word;
create table tag_similar_dict_unique_word as
select distinct word
from (
select word from tag_similar_dict
union all
select similar as word from tag_similar_dict)
t;

