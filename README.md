# clickhouse R DBI client

This R package provides a DBI client for the ClickHouse database, based on https://github.com/hannesmuehleisen/clickhouse-r, but almost completely rewriten.
The problem with original connector for me was that it's unable to work in many cases.


## Installation

* the latest development version from github with:

    ```R
    devtools::install_github("kislenok-roman/clickhouse-r")
    ```
    
* or use original version (not recomended):

    ```R
    devtools::install_github("hannesmuehleisen/clickhouse-r")
    ```


If you encounter a bug, please file a minimal reproducible example on [github](https://github.com/kislenok-roman/clickhouse-r/issues).

## Usage

```R
library(DBI)
con <- dbConnect(clickhouse::clickhouse(), host = "localhost")
dbWriteTable(con, "mtcars", mtcars)
dbListTables(con)
dbGetQuery(con, "SELECT COUNT(*) FROM mtcars")
d <- dbReadTable(con, "mtcars")
dbDisconnect(con)
```

Please note additional features:

### Temporary vs memory to store query results
```R
con1 <- dbConnect(clickhouse(), host = "localhost", use = "file") # default
con2 <- dbConnect(clickhouse(), host = "localhost", use = "memory")
```
You always can use temporary file as it's fast and reliable. Also temporary file allow you
to get a lot of data from server (something up to 10 mln rows). 

### External tables
ClickHouse support external tables and you can use such a table in queries:
```R
dbGetQuery(con, "select a, count() from tbl1 group by a", tbl1 = CJ(a = 1:3, b = 1:2))
```
You can use external table with "join" or "in", please note that it should be a small table to fit in memory as it uses ClickHouse Memory engine. You could use several tables.

### Authentication
ClickHouse HTTP allow you to authenticate using user/password pair, just put them in user/password params of the connection:
```R
con <- dbConnect(clickhouse(), host = "localhost", user = "user", password = "password")
```

### Database selection and other params
ClickHouse allow you to set some connection/query [settings](https://clickhouse.yandex/docs/en/single/index.html#settings). 
The most useful is database -- selects database to execute all queries, can be put when creating connection or in each query:
```R
con <- dbConnect(clickhouse(), host = "localhost", database = "db1")
dbGetQuery(con, "select * from table1", database = "db2")
dbGetQuery(con, "select * from db2.table1") # as in standard SQL
```

## What's next
* loading external tables from files
* rewrite "fetch"
