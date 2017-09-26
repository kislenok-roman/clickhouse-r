postTable <- function(tbl) {
  # TODO: process NA

  tcon <- textConnection("textOutput", open = "w", local = TRUE)
  write.table(tbl, tcon,
              sep = "\t",
              row.names = FALSE,
              col.names = FALSE,
              quote = FALSE) # "without enclosing quotation marks" https://clickhouse.yandex/docs/en/single/index.html#tabseparated
  textOutputValue <- textConnectionValue(tcon)
  close(tcon)

  paste0(textOutputValue, collapse = "\n")
}

unionParams <- function(l1, l2) {
  for (i in seq_along(l2)) {
    name <- names(l2)[i]
    l1[[name]] <- l2[[i]]
  }
  l1
}

prepareConnection <- function(conn, query = NULL, post = NULL, multiform = NULL) {
  if (length(multiform) == 0) {
    multiform <- NULL
  }
  if (length(post) == 0) {
    post <- NULL
  }
  if (!is.null(query) && is.character(query)) {
    query <- list(query = query)
  }
  if (!is.null(query) && !is.null(query[["query"]]) && nchar(curl::curl_escape(query[["query"]])) > 15 * 1000) {
    # long query need to be put in the body
    if (!is.null(post)) {
      stop("You have a very long query that should be put in post, and already set post with data -- unfortunately ClickHouse http interface does not support this")
    } else {
      post <- query[["query"]]
      query[["query"]] <- NULL
    }
  }
  if (!is.null(post) && !is.null(multiform)) {
    stop("Only multiform with external data OR plain post with query/data can be set -- check https://github.com/yandex/ClickHouse/issues/1179")
  }
  if (!is.null(query[["query"]])) {
    # This is related to the problem that CH does not accept any requests without query in params or body
    query <- unionParams(conn@params, query)
  }

  handle <- curl::new_handle()

  headers <- list("X-ClickHouse-User" = conn@auth[["user"]],
                  "X-ClickHouse-Key" = conn@auth[["password"]])

  if (!is.null(post)) {
    if (is.data.frame(post)) {
      post <- postTable(post)
    }
    curl::handle_setopt(handle, post = TRUE, customrequest = "POST", postfields = post)
  } else if (!is.null(multiform)) {
    multiformDelimiter <- digest::digest(Sys.time(), "md5")
    multiformRowend <- "\r\n"
    multiformData <- vector("list", 6L * length(multiform))
    for (i in seq_along(multiform)) {
      name <- names(multiform)[i]
      value <- multiform[[i]]

      # We provide to server data format & structure
      query[[paste0(name, "_format")]] <- "TabSeparated"
      query[[paste0(name, "_structure")]] <- paste(names(value), sapply(head(value), dbDataType, dbObj = conn), collapse = ",")

      multiformData[[6L * (i - 1L) + 1L]] <- paste0("--", multiformDelimiter)
      multiformData[[6L * (i - 1L) + 2L]] <- paste0("Content-Disposition: form-data; name=\"", name, "\"; filename=\"", name, "\".tsv")
      multiformData[[6L * (i - 1L) + 3L]] <- "Content-Type: text/tab-separated-values"
      multiformData[[6L * (i - 1L) + 4L]] <- ""
      multiformData[[6L * (i - 1L) + 5L]] <- postTable(value)
      multiformData[[6L * (i - 1L) + 6L]] <- paste0("--", multiformDelimiter)
    }
    multiformData[[length(multiformData)]] <- paste0(multiformData[[length(multiformData)]], "--")
    multiformData <- paste0(multiformData, collapse = multiformRowend)

    headers[["Content-type"]] <- paste0("multipart/form-data; boundary=", multiformDelimiter)
    headers[["Content-Length"]] <- as.character(nchar(multiformData))
    curl::handle_setopt(handle, post = TRUE, customrequest = "POST", postfields = multiformData)
  }

  curl::handle_setheaders(handle, .list = headers)

  if (length(query) > 0) {
    url <- paste0(conn@url, "?", paste0(names(query), "=", curl::curl_escape(unlist(query)), collapse = "&"))
  } else {
    url <- conn@url
  }

  list(handle = handle, url = url)
}
