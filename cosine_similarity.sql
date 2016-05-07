


--Read data into table for searching
CREATE TABLE table1(rowid BIGINT, text_data STRING) ROW FORMAT DELIMITED fields terminated by ','  lines terminated by '\n' stored as textfile;
load data inpath '/full_data.txt' overwrite into TABLE table1;



--Stop words collection
create table stopwords(word_stop STRING) ROW FORMAT DELIMITED lines terminated by '\n' stored as textfile;
load data inpath 'stopwords.txt' overwrite into TABLE stopwords;	

	
--Find how many odcumenst in the corpus
select count(*) from table1;
--3908755 

--Create table to get the fequency of the terms
drop table ngram_table;
create table ngram_table(id string, NEW_ITEM ARRAY<STRUCT<ngram:array<string>, estfrequency:double>>);
INSERT OVERWRITE TABLE ngram_table SELECT rowid, context_ngrams(sentences(lower(text_data)), array(null), 1000) as word_map FROM table1 group by rowid;

--Create termfrequency table 
create table tf(id BIGINT, term String, tf double) row format delimited collection items terminated by ' ' stored as textfile;
insert overwrite table tf SELECT id, X.ngram[0], X.estfrequency from ngram_table LATERAL VIEW explode(new_item) Z as X;

--Remove stop words
create table tf1(id BIGINT, term String, tf double) row format delimited collection items terminated by ' ' stored as textfile;
insert overwrite table tf1 select id, term, t1.tf from tf t1 LEFT OUTER JOIN stopwords t2 ON (t1.term = t2.word_stop) WHERE t2.word_stop is NULL;

--Compute Document Frequency
create table df(term String, num_doc BIGINT) row format delimited stored as textfile;
insert overwrite table df select term, count(id)  from tf1 group by term;

--Compute TFIDF scores
create table tfidf(docid BIGINT, term STRING, TFIDF_score double) row format delimited stored as textfile;
insert overwrite table tfidf select tf1.id, df.term, (log(10, 3908755)-log(10, df.num_doc)+1)* double(tf1.tf) from tf1, df where tf1.term = df.term;


----Query terms---German Shepherd   Fast Cars   Pretty woman   Great beaches  Short circuit  Car Crash

--Read the  Search string
drop table query_table;
create table query_table(doc_id bigint, doc_text string) row format delimited stored as textfile;
insert overwrite table query_table select 1, "Car Crash" from tf limit 1;

--Get the term frequency vector
drop table q_ngram_table;
create table q_ngram_table(id string, doc string, NEW_ITEM ARRAY<STRUCT<ngram:array<string>, estfrequency:double>>);
INSERT OVERWRITE TABLE q_ngram_table SELECT doc_id, doc_text, context_ngrams(sentences(lower(doc_text)), array(null), 100) as word_map FROM query_table group by doc_id, doc_text;

drop table q_tf;
create table q_tf(id BIGINT, term String, tf double) row format delimited collection items terminated by ' ' stored as textfile;
insert overwrite table q_tf SELECT id, X.ngram[0], X.estfrequency from q_ngram_table LATERAL VIEW explode(new_item) Z as X;

--Get the document frequency
drop table q_df;
create table q_df(term String, num_doc int) row format delimited stored as textfile;
insert overwrite table q_df select term, count(id)  from q_tf group by term;

--Compute TFIDF scores 
drop table q_tfidf;
create table q_tfidf(docid BIGINT, term STRING, TFIDF_score double) row format delimited stored as textfile;
insert overwrite table q_tfidf select q_tf.id, q_df.term, (log(10, 1)-log(10, q_df.num_doc)+1)* double(q_tf.tf) from q_tf, q_df where q_tf.term = q_df.term;

--Cosine similarity - Dot product
drop table cosine_similarity;
create table cosine_similarity(docid BIGINT, cosine_sim double) row format delimited stored as textfile;
insert overwrite table cosine_similarity select lhs.docid, sum(lhs.TFIDF_score * rhs.TFIDF_score)/((sqrt(sum(lhs.TFIDF_score * lhs.TFIDF_score))) * (sqrt(sum(rhs.TFIDF_score * rhs.TFIDF_score))))*1.0 as cos_sim from tfidf as lhs inner join q_tfidf as rhs on lhs.term = rhs.term group by lhs.docid, rhs.docid SORT BY cos_sim DESC LIMIT 5  ;

