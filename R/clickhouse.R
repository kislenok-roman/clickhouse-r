setClass("clickhouse_driver",
         contains = "DBIDriver"
)

# TODO: add database parameter
# You can use the 'database' URL parameter to specify the default database.
# SM: https://clickhouse.yandex/reference_en.html#Settings
# https://clickhouse.yandex/reference_en.html#SET
setClass("clickhouse_connection",
         contains = "DBIConnection",
         slots = list(
           url = "character",
           use = "character",
           auth = "list",
           params = "list",
           cert = "character"
         )
)

setClass("clickhouse_result",
         contains = "DBIResult",
         slots = list(
           sql = "character",
           env = "environment",
           conn = "clickhouse_connection"
         )
)

setMethod("dbIsValid", "clickhouse_driver", function(dbObj, ...) {
  TRUE
})

setMethod("dbUnloadDriver", "clickhouse_driver", function(drv, ...) {
  invisible(TRUE)
})

setMethod("dbIsValid", "clickhouse_connection", function(dbObj, ...) {
  tryCatch({
    # https://clickhouse.yandex/reference_en.html#HTTP interface
    # If you make a GET / request without parameters,
    # it returns the string "Ok" (with a line break at the end).
    # You can use this in health-check scripts.
    http <- prepareConnection(dbObj)

    response <- curl::curl_fetch_memory(http$url, http$handle)
    msg <- rawToChar(response$content)

    if (response$status_code == 200L && msg == "Ok.\n") {
      TRUE
    } else {
      warning(msg)
      FALSE
    }
    TRUE
  }, error = function(e) {
    warning(e)
    FALSE
  })
})

clickhouse <- function() {
  new("clickhouse_driver")
}

setMethod(
  "dbConnect",
  "clickhouse_driver",
  function(drv,
           host = "localhost", port = 8123L, user = "default", password = "",
           database = NULL, certPath = NULL,
           use = c("file", "memory"), ...) {
    use <- match.arg(use)
    url <- paste0("http://", host, ":", port, "/")
    params <- list(...)
    auth <- list()
    if (length(params) > 0 && is.null(names(params))) {
      stop("Only named additional params allowed")
    }
    if (!is.null(certPath)) {
      certPath <- normalizePath(certPath, mustWork = FALSE)
      if (!file.exists(certPath)) {
        stop("Certificate file not found")
      }
    }

    auth[["user"]] <- user
    auth[["password"]] <- password
    if (!missing(database) && !is.null(database) && !is.na(database) && database != "") {
      params[["database"]] = database
    }

    con <- new(
      "clickhouse_connection",
      url = url,
      use = use,
      params = params,
      auth = auth,
      cert = certPath
    )
    stopifnot(dbIsValid(con))
    con
  }
)

setMethod("dbListTables", "clickhouse_connection", function(conn, ...) {
  as.character(dbGetQuery(conn, "SHOW TABLES")[[1]])
})

setMethod("dbExistsTable", "clickhouse_connection", function(conn, name, ...) {
  as.logical(name %in% dbListTables(conn))
})

setMethod("dbReadTable", "clickhouse_connection", function(conn, name,
                                                           limit = NULL, ...) {
  if (is.numeric(limit)) {
    if (limit > 1) {
      limitText <- paste0(" limit ", format(limit, scientific = FALSE))
    } else if (limit > 0) {
      limitText <- paste0(" sample ", format(limit, scientific = FALSE))
    } else {
      limitText <- ""
    }
  } else {
    limitText <- ""
  }
  dbGetQuery(conn, paste0("SELECT * FROM ", name, limitText))
})

setMethod("dbRemoveTable", "clickhouse_connection", function(conn, name, ...) {
  dbExecute(conn, paste0("DROP TABLE ", name))
  invisible(TRUE)
})

setMethod(
  "dbSendQuery",
  "clickhouse_connection",
  function(conn, statement, ...) {
    query <- list(query = sub("[; ]*;\\s*$", "", statement, ignore.case = TRUE, perl = TRUE))
    ext <- list(...)

    has_resultset <- grepl("^\\s*(SELECT|SHOW)\\s+", query$query, perl = TRUE, ignore.case = TRUE)

    if (has_resultset) {
      if (grepl(".*FORMAT\\s+\\w+\\s*$", statement, perl = TRUE, ignore.case = TRUE)) {
        stop("Can't have FORMAT keyword in queries, query ", statement)
      }
      query$query <- paste0(query$query ," FORMAT TabSeparatedWithNames")
    }

    if (length(ext) > 0) {
      # We have more then query - there could be:
      # 1. additional set params to ClickHouse server
      # 2. additional external tables to use in query
      # 3. some thing user put by error
      if (!is.null(names(ext)) && anyDuplicated(names(ext)) == 0) {
        data <- list()

        for (name in names(ext)) {
          if (is.data.frame(ext[[name]])) {
            # external data
            data[[name]] <- ext[[name]]
          } else {
            # just additional parameter
            query <- c(query, ext[name])
          }
        }
      } else {
        stop("Can't use external parameters. Each should be named and there are should be no duplicates")
      }
    } else {
      data <- NULL
    }

    http <- prepareConnection(conn, query, NULL, data)

    if (conn@use == "memory") {
      req <- curl::curl_fetch_memory(http$url, http$handle)
    } else {
      tmp <- tempfile()
      req <- curl::curl_fetch_disk(http$url, tmp, http$handle)
    }

    if (req$status_code != 200) {
      if (conn@use == "memory") {
        stop(rawToChar(req$content))
      } else {
        stop(readLines(tmp))
      }
    }

    dataenv <- new.env(parent = emptyenv())

    if (has_resultset) {
      # try to avoid problems when select just one column that can contain ""
      # without "blank.lines.skip" we'll get warning:
      # Stopped reading at empty line ... but text exists afterwards (discarded): ...
      # and not all rows will be read
      if (conn@use == "memory") {
        tmp <- rawToChar(req$content)
      }
      dataenv$data <- data.table::fread(tmp, sep = "\t",
                                        header = TRUE,
                                        showProgress = FALSE,
                                        blank.lines.skip = TRUE)
      if (conn@use == "file") {
        unlink(tmp)
      }
    }

    dataenv$success <- TRUE
    dataenv$delivered <- -1
    dataenv$open <- TRUE
    dataenv$rows <- nrow(dataenv$data)

    new("clickhouse_result",
        sql = statement,
        env = dataenv,
        conn = conn
    )
  }
)

setMethod("dbWriteTable",
          signature(conn = "clickhouse_connection",
                    name = "character",
                    value = "ANY"),
          definition = function(conn, name, value, overwrite = FALSE, append = FALSE, engine = "TinyLog", ...) {
  if (overwrite && append) {
    stop("Setting both overwrite and append to TRUE makes no sense.")
  }
  if (!is.data.frame(value)) {
    value <- data.table::as.data.table(value)
  }

  if (nrow(value) > 0) {
    isTableExists <- dbExistsTable(conn, name)

    if (isTableExists) {
      if (overwrite) {
        warning("Clickhouse does not support deletition or truncation at all. ",
                "There are possible solution: create a copy of this table without ",
                "data (to preserve structure), drop this table, take another copy ",
                "(to preserve name), drop temporary table. But currently we just ",
                "remove & create new one")
        # TODO: implement proposal solution
        dbRemoveTable(conn, name)
        isTableExists <- FALSE
      } else if (!append) {
        stop("Table ", name, " already exists. Set overwrite = TRUE if you want to ",
             "remove the existing table. Set append=TRUE if you would like to add ",
             "the new data to the existing table.")
      }
    }

    if (!isTableExists) {
      if (missing(engine)) {
        warning("Using default engine TinyLog, try to set MergeTree as engine as it's preferable (but read docs before).")
      }
      cols <- paste(names(value), sapply(head(value), dbDataType, dbObj = conn), collapse = ",")
      dbExecute(conn, paste0("create table ", name, " (", cols, ") engine = ", engine))
    }

    http <- prepareConnection(conn, paste0("insert into ", name, " format TabSeparated"), value)
    req <- curl::curl_fetch_memory(http$url, http$handle)
    if (req$status_code != 200) {
      stop("Error writing data to table ", rawToChar(req$content))
    } else {
      invisible(TRUE)
    }
  } else {
    stop("There are no rows to write")
  }
})

setMethod("dbDataType", signature(dbObj = "clickhouse_connection", obj = "ANY"),
          definition = function(dbObj, obj, ...) {
  objClass <- class(obj)[1]

  if (objClass == "logical") {
    "UInt8"
  } else if (objClass == "integer") {
    "Int32"
  } else if (objClass == "numeric") {
    "Float64"
  } else if (objClass == "Date") {
    "Date"
  } else if (objClass == "DateTime") {
    "POSIXct"
  } else {
    "String"
  }
}, valueClass = "character")

setMethod("dbBegin", "clickhouse_connection", definition = function(conn, ...) {
  stop("Transactions are not supported.")
})

setMethod("dbCommit", "clickhouse_connection", definition = function(conn, ...) {
  stop("Transactions are not supported.")
})

setMethod("dbRollback", "clickhouse_connection", definition = function(conn, ...) {
  stop("Transactions are not supported.")
})

setMethod("dbDisconnect", "clickhouse_connection", function(conn, ...) {
  invisible(TRUE)
})

setMethod("fetch", signature(res = "clickhouse_result", n = "numeric"), definition = function(res, n, ...) {
  # TODO: rewrire this
  if (!dbIsValid(res) || dbHasCompleted(res)) {
    stop("Cannot fetch results from exhausted, closed or invalid response.")
  }
  if (n == 0) {
    stop("Fetch 0 rows? Really?")
  }
  if (res@env$delivered < 0) {
    res@env$delivered <- 0
  }
  if (res@env$delivered >= res@env$rows) {
    return(res@env$data[F,, drop=F])
  }
  if (n > -1) {
    n <- min(n, res@env$rows - res@env$delivered)
    res@env$delivered <- res@env$delivered + n
    return(res@env$data[(res@env$delivered - n + 1):(res@env$delivered),, drop=F])
  }
  else {
    start <- res@env$delivered + 1
    res@env$delivered <- res@env$rows
    return(res@env$data[start:res@env$rows,, drop=F])
  }
})

setMethod("dbGetRowsAffected", "clickhouse_result", definition = function(res, ...) {
  # TODO we can return this if we use "Format JSON"
  as.numeric(NA)
})

setMethod("dbClearResult", "clickhouse_result", definition = function(res, ...) {
  res@env$open <- FALSE
  invisible(TRUE)
})

setMethod("dbHasCompleted", "clickhouse_result", definition = function(res, ...) {
  res@env$delivered >= res@env$rows
})

setMethod("dbIsValid", "clickhouse_result", definition = function(dbObj, ...) {
  dbObj@env$success && dbObj@env$open
})

