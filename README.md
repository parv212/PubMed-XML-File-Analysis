# PubMed-XML-File-Analysis

This GitHub repository details a multi-stage data project, starting with the extraction of data from an XML document and storing it relationally in an SQLite database. The project then progresses to transforming this transactional database into an analytical one using a star schema in MySQL, culminating in querying facts from the analytical database.

Key aspects of the project include:

* **Part 1:** OLTP Database Creation: Creating a normalized relational database from XML data, focusing on articles, journals, and authors.
* **Part 2:** Star/Snowflake Schema Development: Transforming the normalized schema into a de-normalized schema suitable for Online Analytical Processing (OLAP) using MySQL.
* **Part 3:** Data Mining: Employing the OLAP star/snowflake schema for simple data mining tasks, such as identifying top authors and journals.
* **Methodology and Tools:** Utilizing RStudio, SQLite, and MySQL for database management and data transformation.

This project serves as an excellent example of practical data processing, transformation, and analysis in a real-world scenario, demonstrating skills in database management and data mining.
