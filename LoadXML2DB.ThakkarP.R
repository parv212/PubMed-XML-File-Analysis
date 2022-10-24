# Author: Parv Thakkar
# Course: CS5200
# Date: 08/11/2022

# Load Libraries

library(RSQLite)
library(XML)
library(DBI)
library(knitr)
library(stringr)
library(dplyr)

# Connecting to the Database

fpath <- "/Users/parvthakkar/Desktop/NEU MSDS/Database Management Systems/Practicum/Practicum 2/pubmed.db"

dbcon <- dbConnect(RSQLite::SQLite(), fpath)

# Create Tables Part 1 Question 4

dbSendQuery(conn = dbcon, "DROP TABLE IF EXISTS Articles")
dbSendQuery(conn = dbcon, "DROP TABLE IF EXISTS Authors")
dbSendQuery(conn = dbcon, "DROP TABLE IF EXISTS AuthorList")
dbSendQuery(conn = dbcon, "DROP TABLE IF EXISTS Journals")
dbSendQuery(conn = dbcon, "DROP TABLE IF EXISTS JournalIssue")

dbSendQuery(conn = dbcon, "CREATE TABLE Articles
(
articleID integer NOT NULL,
Title text,
Language text,
Medium text,
journalIssueID integer,
PRIMARY KEY (articleID),
FOREIGN KEY (journalIssueID) References JournalIssue(issueID)
)")

dbSendQuery(conn = dbcon, "CREATE TABLE Authors
(
authorID integer NOT NULL,
LastName text,
ForeName text,
Initial text,
PRIMARY KEY (authorID)
)")
            
dbSendQuery(conn = dbcon, "CREATE TABLE AuthorList
(
listID integer NOT NULL,
authorID integer,
articleID integer,
PRIMARY KEY (listID),
FOREIGN KEY (authorID) references Authors(authorID),
FOREIGN KEY (articleID) references Articles(articleID)
)")
            
dbSendQuery(conn = dbcon, "CREATE TABLE Journals
(
journalID integer NOT NULL,
Title text,
PRIMARY KEY (journalID)
)")
            
dbSendQuery(conn = dbcon, "CREATE TABLE JournalIssue
(
issueID integer NOT NULL,
Volume integer,
Issue integer,
Date date,
journalID integer,
PRIMARY KEY (issueID),
FOREIGN KEY (journalID) references Journals(journalID)
)")

# Parsing XML Part 1 Question 5

xmlObj <- xmlParse("pubmed-tfm-xml/pubmed22n0001-tf.xml")
root <- xmlRoot(xmlObj)

numArticle <- xmlSize(root)

# Creating Dataframes to store added additional columns to fill the foreign key values

articles.df <- data.frame(articleID = integer(),
                          Title = character(),
                          Language = character(),
                          Medium = character(),
                          journalIssueID = integer(),
                          volume = integer(),
                          issueno = integer(),
                          date = character()
                          )

journals.df <- data.frame(journalID = integer(),
                          Title = character())

authors.df <- data.frame(authorID = integer(),
                         LastName = character(),
                         ForeName = character(),
                         Initial = character())

list.df <- data.frame(listID = integer(),
                      authorID = integer(),
                      articleID = integer(),
                      LastName = character(),
                      ForeName = character(),
                      Initial = character())

issue.df <- data.frame(issueID = integer(),
                       Volume = integer(),
                       Issue = integer(),
                       Date = character(),
                       journalID = integer(),
                       journalTitle = character())

# Parse Date

parseDate <- function(dateNode) {
  if(!is.null(dateNode[["MedlineDate"]])) {
    date = xmlValue(dateNode[["MedlineDate"]])
    date = str_replace(str_sub(date, 1, 8), " ", "-")
  } else  {
    date = xmlValue(dateNode[["Year"]])
    if(!is.null(dateNode[["Month"]])) {
      date = paste0(date, "-", xmlValue(dateNode[["Month"]]))
    }
  }
  
  if(!is.null(dateNode[["Day"]])) {
    date = paste0(date, "-", xmlValue(dateNode[["Day"]]))
  } else  {
    date = paste0(date, "-", "01")
  }
  
  if(str_length(date) < 10) {
    date = ""
  } else  {
    date = format(as.Date(date, "%Y-%b-%d"), "%Y-%m-%d")
  }
  return(date)
}

# Parse the whole XML

pubmedXpath <- "/PubmedArticleSet/PubmedArticle"
ArticleListN <- xpathSApply(xmlObj, pubmedXpath)

# creating Surrogate keys for each table. Did not use PMID as 
# it is not continuous total rows 30000 but PMID goes till 30900(approx.)

journalid <- 1
issueid <- 1
listid <- 1
authorid <- 1
articleid <- 1

for (i in 1:numArticle) {
  
  # Extracting article from list of articles
  ArticleList <- ArticleListN[[i]]
  
  article <- ArticleList[["Article"]]
  journal <- article[["Journal"]]
  issue <- journal[["JournalIssue"]]
  
  # Filling article tale values
  # pmid <- as.integer(xmlGetAttr(ArticleList, "PMID"))
  articles.df[i, "articleID"] <- articleid
  language <- as.character(getChildrenStrings(article[["Language"]]))
  articles.df$Language[i] <- language
  title <- as.character(getChildrenStrings(article[["ArticleTitle"]]))
  articles.df$Title[i] <- title
  
  medium <- xmlGetAttr(journal[["JournalIssue"]], "CitedMedium")
  articles.df$Medium[i] <- medium
  
  # Filling journal table values
  # ISSN not included as it is different for different medium
  journals.df[i, "journalID"] <- journalid
  issue.df[i, "journalID"] <- journalid
  journalid <- journalid + 1
  
  journalTitle <- as.character(getChildrenStrings(journal[["Title"]]))
  journals.df[i, "Title"] <- journalTitle
  issue.df[i, "journalTitle"] <- journalTitle
  
  # Filling Issue table values
  # Checked Volume and Issues for letters and assigned them 0
  # Also handled values having - in them
  # SImilar actions for the Issue table
  vol <- xmlValue(issue[["Volume"]])
  
  if(!is.na(vol))  {
    volume <- as.character(getChildrenStrings(issue[["Volume"]]))
    if(grepl("\\D", volume)) {
      volume <- 0
    } else if(grepl("-", volume))  {
      volume <- str_sub(volume, 1, 1)
    } else  {
      volume <- as.integer(volume)
    }
  }
  
  issue.df$issueID[i] <- issueid
  articles.df[i, "journalIssueID"] <- issueid
  issueid <- issueid + 1
  issue.df$Volume[i] <- volume
  articles.df[i, "volume"] <- volume
  
  iss <- xmlValue(issue[["Issue"]])
  
  if(!is.na(iss)) {
    issueno <- as.character(getChildrenStrings(issue[["Issue"]]))
    if(grepl("\\D", issueno)) {
      issueno <- 0
    } else if(grepl("-", issueno))  {
      issueno <- str_sub(issueno, 1, 1)
    } else  {
      issueno <- as.integer(issueno)
    }
  }
  
  issue.df$Issue[i] <- issueno
  articles.df[i, "issueno"] <- issueno
  
  date <- parseDate(issue[["PubDate"]])
  issue.df$Date[i] <- as.character(date)
  articles.df[i, "date"] <- as.character(date)
  
  # Filling the author table values by iterating each author list
  if(!is.null(article[["AuthorList"]])) {
    authorList <- xmlChildren(article[["AuthorList"]])
    
    for (author in authorList)  {
      
      ln <- xmlValue(author[["LastName"]])
      
      if(!is.na(ln))  {
        lastname <- as.character(getChildrenStrings(author[["LastName"]]))
        authors.df[authorid, "LastName"] <- lastname
        list.df[authorid, "LastName"] <- lastname
      }
      
      fn <- xmlValue(author[["ForeName"]])
      
      if(!is.na(fn))  {
        forename <- as.character(getChildrenStrings(author[["ForeName"]]))
        authors.df[authorid, "ForeName"] <- forename
        list.df[authorid, "ForeName"] <- forename
      }
      
      ini <- xmlValue(author[["Initials"]])
      
      if(!is.na(ini))  {
        initial <- as.character(getChildrenStrings(author[["Initials"]]))
        authors.df[authorid, "Initial"] <- initial
        list.df[authorid, "Initial"] <- initial
      }
      
      cn <- xmlValue(author[["CollectiveName"]])
      
      if(!is.na(cn))  {
        lastname <- as.character(getChildrenStrings(author[["CollectiveName"]]))
        authors.df[authorid, "LastName"] <- lastname
        list.df[authorid, "LastName"] <- lastname
      }
      list.df[authorid, "listID"] <- authorid
      list.df[authorid, "articleID"] <- articleid
      list.df[authorid, "authorID"] <- authorid
      
      authors.df[authorid, "authorID"] <- authorid
      authorid <- authorid + 1
    }
  }
  articleid <- articleid + 1
}

# Removing duplicates from journal table and also modifying the id respectively

unique_journal.df <- journals.df %>% distinct(Title, .keep_all = T)

for(row in 1:nrow(unique_journal.df)) {
  unique_journal.df$journalID[row] <- row
}

dbWriteTable(dbcon, "Journals", unique_journal.df, overwrite = T, row.names = F)

# Removing duplicates from issue table
issue.df <- na.omit(issue.df)
unique_issue.df <- issue.df %>% distinct(Volume, Issue, Date, .keep_all = T)

for(row in 1:nrow(unique_issue.df)) {
  unique_issue.df$issueID[row] <- row
  unique_issue.df$journalID[row] <- filter(unique_journal.df, Title == unique_issue.df$journalTitle[row])$journalID
}

issue_final <- unique_issue.df %>% select(issueID, Volume, Issue, Date, journalID)

dbWriteTable(dbcon, "JournalIssue", issue_final, overwrite = T, row.names = F)

# Adding modified journal ID to Article table
articles.df <- na.omit(articles.df)

for(row in 1:nrow(articles.df)) {
  articles.df$journalIssueID[row] <- filter(unique_issue.df, Volume == articles.df$volume[row]&
                                         Issue == articles.df$issueno[row]&
                                         Date == articles.df$date[row])$issueID
}

article_final <- articles.df %>% select(articleID, Title, Language, Medium, journalIssueID)

dbWriteTable(dbcon, "Articles", article_final, overwrite = T, row.names = F)

# Removing duplicates from author table

authors.df <- na.omit(authors.df)
unique_author.df <- authors.df %>% distinct(LastName, ForeName, Initial, .keep_all = T)

for(row in 1:nrow(unique_author.df))  {
  authors.df$authorID[row] <- row
}

dbWriteTable(dbcon, "Authors", unique_author.df, overwrite = T, row.names = F)

# Modifying the author list table accordingly

list.df_new <- na.omit(list.df)

for(row in 1:nrow(list.df_new)) {
  list.df_new$authorID[row] <- filter(unique_author.df,
                                      LastName == list.df_new$LastName[row]&
                                      ForeName == list.df_new$ForeName[row]&
                                      Initial == list.df_new$Initial[row])$authorID
  
  list.df_new$listID[row] <- row
}

list_final <- list.df_new %>% select(listID, authorID, articleID)

dbWriteTable(dbcon, "AuthorList", list_final, overwrite = T, row.names = F)

# Disconnecting database
dbDisconnect(dbcon)