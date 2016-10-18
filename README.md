# clickhouse R DBI client

This R package provides a DBI client for the ClickHouse database

## Installation

* the latest development version from github with

    ```R
    devtools::install_github("hannesmuehleisen/clickhouse-r")
    ```

If you encounter a bug, please file a minimal reproducible example on [github](https://hannesmuehleisen/clickhouse-r/issues).

## Usage

```R
library(DBI)
con <- dbConnect(clickhouse::clickhouse())
dbWriteTable(con, "iris", mtcars)
dbListTables(con)
dbGetQuery(con, "SELECT COUNT(*) FROM mtcars")
d <- dbReadTable(con, "mtcars")
dbDisconnect(con)
```
