# clickhouse R DBI client

This R package provides a DBI client for the ClickHouse database

## Installation

* the latest development version from github with

    ```R
    devtools::install_github("hannesmuehleisen/clickhouse-r")
    ```

If you encounter a bug, please file a minimal reproducible example on [github](https://github.com/hannesmuehleisen/clickhouse-r/issues).

## Usage

```R
library(DBI)
con <- dbConnect(clickhouse::clickhouse(), host="localhost", port=8123L, user="default", password="")
dbWriteTable(con, "mtcars", mtcars)
dbListTables(con)
dbGetQuery(con, "SELECT COUNT(*) FROM mtcars")
d <- dbReadTable(con, "mtcars")
dbDisconnect(con)
```

Please note additional features:

*temporary vs memory to store query results*
```R
con1 <- dbConnect(clickhouse(), host = "localhost", use = "temp") # default
con2 <- dbConnect(clickhouse(), host = "localhost", use = "memory") # default
```
You always can use temporary file as it's fast and reliable. Also temporary file allow
to get a lot of data from server. 

*external tables*
ClickHouse support external tables and you can use such a table in queries:
```R
dbGetQuery(con, "select a, count() from tbl1 group by a", tbl1 = CJ(a = 1:3, b = 1:2))
```
You can use such table with join/in, please note that it should be a small table to fit in memory as it uses Memory engine. You could use several tables.

*what's next*
* using tables from files
