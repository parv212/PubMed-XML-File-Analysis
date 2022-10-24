# Author: Parv Thakkar
# Course: CS5200
# Date: 08/11/2022

# Load Libraries

library(RMySQL)
library(DBI)
library(knitr)
library(stringr)
library(dplyr)

# Connecting to MySQL database

db_user <- 'admin'
db_password <- 'khoury5200'
db_name <- 'practicum2'
db_host <- 'practicum2.clwtsa2zfujt.us-east-2.rds.amazonaws.com'
db_port <- 3306

mydb <-  dbConnect(RMySQL::MySQL(), user = db_user, password = db_password,
                   dbname = db_name, host = db_host, port = db_port)

# Connecting to the SQLite Database

fpath <- "/Users/parvthakkar/Desktop/NEU MSDS/Database Management Systems/Practicum/Practicum 2/pubmed.db"

dbcon <- dbConnect(RSQLite::SQLite(), fpath)

dbSendQuery(conn = mydb, "DROP TABLE IF EXISTS Journal_Fact")
dbSendQuery(conn = mydb, "DROP TABLE IF EXISTS Author_Fact")

# Author Fact Table Process

author_list <- dbGetQuery(dbcon, "select a.authorID, a.LastName, a.ForeName,
                          a.Initial, al.articleID
                          from Authors a inner join AuthorList al on
                          al.authorID = a.authorID")

dbWriteTable(mydb, "author_list", author_list, overwrite = T, row.names = F)

author_count <- dbGetQuery(mydb, "select authorID, LastName, ForeName, Initial,
                           count(articleID) as Num_of_article
                           from author_list
                           group by authorID")

dbWriteTable(dbcon, "author_list", author_count, overwrite = T, row.names = F)

co_author_count <- dbGetQuery(dbcon, "select al.*, a.coauthorCount
                                      from AuthorList al inner join 
                                      (select *, count(articleID)-1 as coauthorCount  
                                        from AuthorList group by articleID) a on 
                                        a.articleID = al.articleID")

dbWriteTable(dbcon, "coauthor", co_author_count, overwrite = T, row.names = F)

author_fact <- dbGetQuery(dbcon, "select ac.*, sum(c.coauthorCount) as CoAuthor_Count
                          from author_list ac inner join coauthor c on
                          c.authorID = ac.authorID
                          group by ac.authorID")

# Writing the Final Author Fact Table

dbWriteTable(mydb, "Author_Fact", author_fact, overwrite = T, row.names=F)

# Journal Fact Table Process

article_issue <- dbGetQuery(dbcon, "select a.articleID, i.Date, i.journalID
                            from Articles a inner join JournalIssue i on
                            i.issueID = a.journalIssueID")

article_issue <- article_issue %>% distinct()

dbWriteTable(dbcon, "article_issue", article_issue, overwrite = T, row.names = F)

journal_issue <- dbGetQuery(dbcon, "select j.journalID, j.Title,
                            DATE(i.Date) as Date, i.articleID
                            from Journals j inner join article_issue i on
                            i.journalID = j.journalID")

dbWriteTable(mydb, "journal_article", journal_issue, overwrite = T, row.names = F)

journal_final <- dbGetQuery(mydb, "select journalID, articleID, Title,
                            YEAR(Date) as Year, QUARTER(Date) as Quarter,
                            MONTH(Date) as Month
                            from journal_article")

dbWriteTable(mydb, "journal_article", journal_final, overwrite = T, row.names = F)

journal_f <- dbGetQuery(mydb, "select Title, Year, Quarter, Month,
                        count(articleID) OVER(PARTITION BY Title, Year) as num_of_article_year,
                        count(articleID) OVER(PARTITION BY Title, Year, Quarter) as num_of_article_quarter,
                        count(articleID) OVER(PARTITION BY Title, Year, Month) as num_of_article_month
                        from journal_article")

journal_f <- na.omit(journal_f)

journal_f <- journal_f %>% distinct()

# Writing the final Journal Fact Table

dbWriteTable(mydb, "Journal_Fact", journal_f, overwrite = T, row.names = F)

# Disconnecting the Database
dbDisconnect(mydb)
dbDisconnect(dbcon)