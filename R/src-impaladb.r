#' Connect to ImpalaDB (http://www.impaladb.org), an Open Source analytics-focused database
#'
#' Use \code{src_impaladb} to connect to an existing ImpalaDB database,
#' and \code{tbl} to connect to tables within that database. Please note that the ORDER BY, LIMIT and OFFSET keywords
#' are not supported in the query when using \code{tbl} on a connection to a ImpalaDB database.
#' If you are running a local database, you only need to define the name of the database you want to connect to.
#' ImpalaDB does nto support anti-joins.
#' Table loading will require rhdfs - https://github.com/RevolutionAnalytics/RHadoop/wiki/rhdfs
#'
#' @template db-info
#' @param dbname Database name
#' @param host,port Host name and port number of database (defaults to localhost:21050)
#' @param user,password User name and password (if needed)
#' @param opts=list() a list of options passed to the ImpalaDB driver - defaults to opts=list(auth="noSasl")
#' @param ... for the src, other arguments passed on to the underlying
#'   database connector, \code{dbConnect}.
#' @param src a ImpalaDB src created with \code{src_impaladb}.
#' @param from Either a string giving the name of table in database, or
#'   \code{\link{sql}} described a derived table or compound join.
#' @export
#' @examples
#' \dontrun{
#' # Connection basics ---------------------------------------------------------
#' # optionally set the class path for the ImpalaDB JDBC connector - alternatively use the CLASSPATH
#' # environment variable
#' options(dplyr.jdbc.classpath = '/path/to/impala-jdbc')
#' # To connect to a database first create a src:
#' my_db <- src_impaladb(dbname="demo")
#' # Then reference a tbl within that src
#' my_tbl <- tbl(my_db, "my_table")
#' }
#'
#' # Here we'll use the Lahman database: to create your own local copy,
#' # create a local database called "lahman" first.
#'
#' if (has_lahman("impaladb")) {
#' # Methods -------------------------------------------------------------------
#' batting <- tbl(lahman_impaladb(), "Batting")
#' dim(batting)
#' colnames(batting)
#' head(batting)
#'
#' # Data manipulation verbs ---------------------------------------------------
#' filter(batting, yearID > 2005, G > 130)
#' select(batting, playerID:lgID)
#' arrange(batting, playerID, desc(yearID))
#' summarise(batting, G = mean(G), n = n())
#' mutate(batting, rbi2 = if(is.null(AB)) 1.0 * R / AB else 0)
#'
#' # note that all operations are lazy: they don't do anything until you
#' # request the data, either by `print()`ing it (which shows the first ten
#' # rows), by looking at the `head()`, or `collect()` the results locally.
#'
#' system.time(recent <- filter(batting, yearID > 2010))
#' system.time(collect(recent))
#'
#' # Group by operations -------------------------------------------------------
#' # To perform operations by group, create a grouped object with group_by
#' players <- group_by(batting, playerID)
#' group_size(players)
#' summarise(players, mean_g = mean(G), best_ab = max(AB))
#'
#' # When you group by multiple level, each summarise peels off one level
#' per_year <- group_by(batting, playerID, yearID)
#' stints <- summarise(per_year, stints = max(stint))
#' filter(stints, stints > 3)
#' summarise(stints, max(stints))
#'
#' # Joins ---------------------------------------------------------------------
#' player_info <- select(tbl(lahman_impaladb(), "Master"), playerID, hofID,
#'   birthYear)
#' hof <- select(filter(tbl(lahman_impaladb(), "HallOfFame"), inducted == "Y"),
#'  hofID, votedBy, category)
#'
#' # Match players and their hall of fame data
#' inner_join(player_info, hof)
#' # Keep all players, match hof data where available
#' left_join(player_info, hof)
#' # Find only players in hof
#' semi_join(player_info, hof)
#' # ImpalaDB cannot do anti-joins
#'
#' # Arbitrary SQL -------------------------------------------------------------
#' # You can also provide sql as is, using the sql function:
#' batting2008 <- tbl(lahman_impaladb(),
#'   sql('SELECT * FROM "Batting" WHERE "yearID" = 2008'))
#' batting2008
#' }

# helper functions from dplyr

"%||%" <- function(x, y) if(is.null(x)) y else x


auto_names <- function(x) {
  nms <- names2(x)
  missing <- nms == ""
  if (all(!missing)) return(nms)

  deparse2 <- function(x) paste(deparse(x, 500L), collapse = "")
  defaults <- vapply(x[missing], deparse2, character(1), USE.NAMES = FALSE)

  nms[missing] <- defaults
  nms
}

unique_name <- local({
  i <- 0

  function() {
    i <<- i + 1
    paste0("_W", i)
  }
})

unique_names <- function(x_names, y_names, by, x_suffix = ".x", y_suffix = ".y") {
  common <- setdiff(intersect(x_names, y_names), by)
  if (length(common) == 0) return(NULL)

  x_match <- match(common, x_names)
  x_new <- x_names
  x_new[x_match] <- paste0(x_names[x_match], x_suffix)

  y_match <- match(common, y_names)
  y_new <- y_names
  y_new[y_match] <- paste0(y_names[y_match], y_suffix)

  list(x = setNames(x_new, x_names), y = setNames(y_new, y_names))
}


common_by <- function(by = NULL, x, y) {
  if (is.list(by)) return(by)

  if (!is.null(by)) {
    x <- names(by) %||% by
    y <- unname(by)

    # If x partially named, assume unnamed are the same in both tables
    x[x == ""] <- y[x == ""]

    return(list(x = x, y = y))
  }

  by <- intersect(tbl_vars(x), tbl_vars(y))
  if (length(by) == 0) {
    stop("No common variables. Please specify `by` param.", call. = FALSE)
  }
  message("Joining by: ", capture.output(dput(by)))

  list(
    x = by,
    y = by
  )
}


sql_vector <- function(x, parens = NA, collapse = " ", con = NULL) {
  if (is.na(parens)) {
    parens <- length(x) > 1L
  }

  x <- names_to_as(x, con = con)
  x <- paste(x, collapse = collapse)
  if (parens) x <- paste0("(", x, ")")
  sql(x)
}

names2 <- function(x) {
  names(x) %||% rep("", length(x))
}

names_to_as <- function(x, con = NULL) {
  names <- names2(x)
  as <- ifelse(names == '', '', paste0(' AS ', sql_escape_ident(con, names)))

  paste0(x, as)
}

expandAndCheckClassPath <- function(classpath=NULL,
         driverclass="org.apache.hive.jdbc.HiveDriver", jarfiles=c("commons-logging.*.jar",
                                                                   "hadoop-common.jar",
                                                                   "hive-jdbc.*.jar",
                                                                   "hive-common.*.jar",
                                                                   "hive-metastore.*.jar",
                                                                   "hive-service.*.jar",
                                                                   "libfb303.*.jar",
                                                                   "libthrift.*.jar",
                                                                   "commons-httpclient.*.jar",
                                                                   "httpclient.*.jar",
                                                                   "httpcore.*.jar",
                                                                   "log4j.*.jar",
                                                                   "slf4j-api.*.jar",
                                                                   "slf4j-log4j.*.jar",
                                                                   "hive-exec.jar")) {

  if (is.null(classpath)) classpath <- getOption('dplyr.jdbc.classpath', NULL)
  if (is.null(classpath)) classpath <- unname(Sys.getenv("CLASSPATH"))
  classpath <- unlist(strsplit(classpath, ":"))

  jar.search.path <- c(classpath,
                       ".",
                       Sys.getenv("CLASSPATH"),
                       Sys.getenv("PATH"),
                       if (.Platform$OS == "windows") {
                         file.path(Sys.getenv("PROGRAMFILES"), "Hive")
                       } else c("/usr/lib/impala", "/usr/lib/hive/lib", "/usr/lib/hadoop/lib"))

  classpath <- lapply(jarfiles,
                      function (x) { head(list.files(path=list.files(path=jar.search.path, full.names=TRUE, all.files=TRUE),
                                                     pattern=paste0("^",x,"$"), full.names=TRUE), 1)})
  do.call(paste, c(as.list(classpath), sep=":"))
}


src_impaladb <- function(dbname, host = "localhost", port = 21050L, user = "", password = "", opts=list(auth="noSasl"), ...) {

  if (!require("dplyr")) {
    stop("dplyr package required to connect to ImpalaDB", call. = FALSE)
  }

  if (!require("RJDBC")) {
    stop("RJDBC package required to connect to ImpalaDB", call. = FALSE)
  }

  if (!require("testthat")) {
    stop("testthat package required to connect to ImpalaDB", call. = FALSE)
  }

  if (!require("lazy")) {
    stop("lazy package required to connect to ImpalaDB", call. = FALSE)
  }
  driverclass <- "org.apache.hive.jdbc.HiveDriver"
  if (length(names(opts)) > 0) {
    opts <- paste0(";", paste(lapply(names(opts), function(x){paste(x,opts[x], sep="=")}), collapse=";"))
  }
  else {
    opts <- ""
  }

  url <- paste0("jdbc:hive2://", host, ":", as.character(port), "/", dbname, opts)
  con <- dbConnect(JDBC(driverclass,
            expandAndCheckClassPath(driverclass=driverclass),
            identifier.quote='`'), url, ...)
  res <- dbGetQuery(con, 'SELECT version() AS version') # do this instead of dbGetInfo(con) - returns nothing because of RJDBC!
  info <- list(dbname=dbname, url=url, version=res$version)

  # stash the dbname on the connection for things like db_list_tables
  attr(con, 'dbname') <- dbname

  # I don't trust the connector - so switch the database
  qry_run_once(con, paste("USE ", dbname))

  env <- environment()
  # temporarily suppress the warning messages from getPackageName()
  wmsg <- getOption('warn')
  options(warn=-1)
  # bless JDBCConnection into ImaplaDB Connection class - this way we can give the JDBC connection
  # driver specific powers
  ImpalaDBConnection <- methods::setRefClass("ImpalaDBConnection", contains = c("JDBCConnection"), where = env)
  options(warn=wmsg)
  con <- structure(con, class = c("ImpalaDBConnection", "JDBCConnection"))

  # Creates an environment that disconnects the database when it's
  # garbage collected
  db_disconnector <- function(con, name, quiet = FALSE) {
    reg.finalizer(environment(), function(...) {
      if (!quiet) {
        message("Auto-disconnecting ", name, " connection ",
          "(", paste(con@dbname, collapse = ", "), ")")
      }
      dbDisconnect(con)
    })
    environment()
  }

  src_sql("impaladb", con,
    info = info, disco = db_disconnector(con, "impaladb", ))
}

#' @export
src_desc.src_impaladb <- function(x) {
  info <- x$info
  paste0("ImpalaDB ", "serverVersion: ", info$version, " [",  info$url, "]\n")
}

double_escape <- function(x) {
  structure(x, class = c("sql", "sql", "character"))
}

#' @export
sql_semi_join.ImpalaDBConnection <- function(con, x, y, anti = FALSE, by = NULL, ...) {
  by <- common_by(by, x, y)
  left <- escape(ident("_LEFT"), con = con)
  right <- escape(ident("_RIGHT"), con = con)
  on <- sql_vector(paste0(
    left, ".", sql_escape_ident(con, by$x), " = ", right, ".", sql_escape_ident(con, by$y)),
    collapse = " AND ", parens = TRUE)

  # with a LEFT SEMI JOIN we can only have the LEFT column values returned
  cols <- x$select
  col_names <- lapply(cols, function (col) {if (length(grep(col, x=x$select)) >= 1) { "_LEFT" } else if (length(grep(col, x=y$select)) >= 1) { "_RIGHT" } else NULL})

  # set the alias attribute for result columns
  pieces <- mapply(function(x, alias) {
      return(paste(sql_quote(alias, "`"), sql_quote(x, "`"), sep="."))
    }, as.character(cols), col_names)
  fields <- do.call(function(...) {paste(..., sep=", ")}, as.list(pieces))
  # double escape so that list of fields do not get quoted
  fields <- double_escape(fields)

  from <- build_sql(
    'SELECT ', fields, ' FROM ', sql_subquery(con, x$query$sql, "_LEFT"), '\n\n',
    'LEFT SEMI JOIN \n',
    sql_subquery(con, y$query$sql, "_RIGHT"), '\n',
    ' ON ', on
  )
  attr(from, "vars") <- x$select
  from
}

#' @export
sql_join.ImpalaDBConnection <- function(con, x, y, type = "inner", by = NULL, ...) {
  join <- switch(type,
    left = sql("LEFT"),
    inner = sql("INNER"),
    right = sql("RIGHT"),
    full = sql("FULL"),
    stop("Unknown join type:", type, call. = FALSE)
  )
  by <- common_by(by, x, y)
  using <- all(by$x == by$y)

  # Ensure tables have unique names
  x_names <- auto_names(x$select)
  y_names <- auto_names(y$select)
  uniques <- unique_names(x_names, y_names, by$x[by$x == by$y])

  if (is.null(uniques)) {
    sel_vars <- c(x_names, y_names)
  } else {
    x <- update(x, select = setNames(x$select, uniques$x))
    y <- update(y, select = setNames(y$select, uniques$y))

    by$x <- unname(uniques$x[by$x])
    by$y <- unname(uniques$y[by$y])

    sel_vars <- unique(c(uniques$x, uniques$y))
  }

  if (using) {
    cond <- build_sql("USING ", lapply(by$x, ident), con = con)
  } else {
    on <- sql_vector(paste0(sql_escape_ident(con, by$x), " = ", sql_escape_ident(con, by$y)),
      collapse = " AND ", parens = TRUE)
    cond <- build_sql("ON ", on, con = con)
  }

  left <- unique_name()
  right <- unique_name()
  cols <- unique(c(x_names, y_names))
  col_names <- lapply(cols, function (col) {if (length(grep(col, x=x_names)) >= 1) { left } else if (length(grep(col, x=y_names)) >= 1) { right } else NULL})

  # set the alias attribute for result columns
  pieces <- mapply(function(x, alias) {
      return(paste(sql_quote(alias, "`"), sql_quote(x, "`"), sep="."))
    }, as.character(cols), col_names)
  fields <- do.call(function(...) {paste(..., sep=", ")}, as.list(pieces))

  # double escape so that list of fields do not get quoted
  fields <- double_escape(fields)

  from <- build_sql(
    'SELECT ', fields, ' FROM ',
    sql_subquery(con, x$query$sql, left), "\n\n",
    join, " JOIN \n\n" ,
    sql_subquery(con, y$query$sql, right), "\n\n",
    cond, con = con
  )
  attr(from, "vars") <- lapply(sel_vars, as.name)

  from
}


# Run a query, abandoning results
qry_run_once <- function(con, sql, data = NULL, in_transaction = FALSE,
                    show = getOption("dplyr.show_sql", default=FALSE),
                    explain = getOption("dplyr.explain_sql", default=FALSE)) {
  if (show) message(sql)
  if (explain) message(db_explain(con, sql))

  if (in_transaction) {
    dbBeginTransaction(con)
    on.exit(dbCommit(con))
  }

  if (is.null(data)) {
    res <- dbSendUpdate(con, as.character(sql))
  } else {
    res <- dbSendPreparedQuery(con, sql, bind.data = data)
  }
  if (!is.null(res)) {
    dbClearResult(res)
  }

  invisible(NULL)
}


sql_create_table <- function(con, table, types, temporary = FALSE) {
  assert_that(is.string(table), is.character(types))
  field_names <- escape(ident(names(types)), collapse = NULL, con = con)
  fields <- sql_vector(paste0(field_names, " ", types), parens = TRUE,
    collapse = ", ", con = con)
  sql <- build_sql("CREATE ", if (temporary) sql("TEMPORARY "),
    "TABLE ", ident(table), " ", fields, " ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n'", con = con)
  qry_run_once(con, sql)
}


copy_to.src_impaladb <- function(dest, df, name = deparse(substitute(df)),
                            types = NULL, temporary = FALSE, indexes = NULL,
                            analyze = TRUE, ...) {
  # ImpalaDB can't create temporary tables

  assert_that(is.data.frame(df), is.string(name), is.flag(temporary))
  if (isTRUE(db_has_table(dest$con, name))) {
    stop("Table ", name, " already exists.", call. = FALSE)
  }

  types <- types %||% db_data_type(dest$con, df)
  names(types) <- names(df)

  con <- dest$con

  db_begin(con)
  sql_create_table(con, name, types, temporary = temporary)
  db_insert_into(con, name, df)
  if (analyze) db_analyze(con, name)
  db_commit(con)
  tbl(dest, name)
}

#' @export
#' @rdname src_impaladb
tbl.src_impaladb <- function(src, from, ...) {
  tbl_sql("impaladb", src = src, from = from, ...)
}

#' @export
src_translate_env.src_impaladb <- function(x) {
  sql_variant(
    base_scalar,
    sql_translator(.parent = base_agg,
      n = function() sql("COUNT(*)"),
      sd =  sql_prefix("STDDEV_SAMP"),
      var = sql_prefix("VAR_SAMP"),
      median = sql_prefix("MEDIAN")
    )
  )
}

#' @export
db_has_table.ImpalaDBConnection <- function(con, table) {
  # ImpalaDB lower cases the table names silently on creation
  tolower(table) %in% db_list_tables(con)
}

#' @export
db_list_tables.ImpalaDBConnection <- function(con) {
   tables <- dbGetTables(con)
   # use dbGetTables instead of dbListTables as this is not scoped by dbname
   # and we do cannot tell what they belong to
   tables <- tables[tables$TABLE_SCHEM == attr(con, 'dbname'),'TABLE_NAME']
   tables
}

#' @export
db_begin.ImpalaDBConnection <- function(con) {
  return(TRUE)
}

#' @export
db_commit.ImpalaDBConnection <- function(con) {
  invisible(TRUE)
}

#' @export
db_insert_into.ImpalaDBConnection <- function(con, table, values) {
  # Convert factors to strings
  is_factor <- vapply(values, is.factor, logical(1))
  values[is_factor] <- lapply(values[is_factor], as.character)

  # Encode special characters in strings
  is_char <- vapply(values, is.character, logical(1))
  values[is_char] <- lapply(values[is_char], encodeString)

  tmp <- tempfile(fileext = ".csv")
  write.table(values, tmp, sep = "\t", quote = FALSE,
    row.names = FALSE, col.names = FALSE,na="")

  if (!require("rhdfs")) {
    stop("rhdfs package required to connect to store data in ImpalaDB", call. = FALSE)
  }
  if (is.null(Sys.getenv("HADOOP_CMD"))) {
    stop("rhdfs package requires HADOOP_CMD environment variable to be set eg: HADOOP_CMD=/usr/bin/hadoop", call. = FALSE)
  }
  hdfs.init()
  hdfs.put(tmp, '/tmp')
  tmp <- paste0('/tmp/', tail(unlist(strsplit(tmp, '/')), n=1))

  sql <- build_sql("LOAD DATA INPATH ", tmp," INTO TABLE ", ident(table), con = con)
  qry_run_once(con, sql)

  invisible()
}

#' @export
db_data_type.ImpalaDBConnection <- function(con, fields) {

  data_type <- function(x) {
    switch(class(x)[1],
      logical = "boolean",
      integer = "integer",
      numeric = "double",
      factor =  "STRING",
      character = "STRING",
      Date =    "TIMESTAMP",
      POSIXct = "TIMESTAMP",
      stop("Unknown class ", paste(class(x), collapse = "/"), call. = FALSE)
    )
  }
  vapply(fields, data_type, character(1))
}

#' @export
db_analyze.ImpalaDBConnection <- function(con, table) {
  sql <- build_sql("COMPUTE STATS ", ident(table), con = con)
  qry_run_once(con, sql)
}

#' @export
db_explain.ImpalaDBConnection <- function(con, sql, ...) {
  exsql <- build_sql("EXPLAIN ", sql, con = con)
  expl <- dbGetQuery(con, exsql)
  out <- capture.output(print(expl))

  paste(out, collapse = "\n")
}

#' @export
db_create_index.ImpalaDBConnection <- function(con, table, columns, name = NULL, ...) {
  # ImpalaDB does not benefit from indices
  invisible(TRUE)
}

#' @export
db_query_fields.ImpalaDBConnection <- function(con, from) {
  # doesn't like the ; on the end
  qry <- dbGetQuery(con, build_sql("SELECT * FROM ", from, " WHERE 0=1", con = con))
  names(qry)
}

#' @export
table_fields.ImpalaDBConnection <- function(con, table) qry_fields(con, table)

# res_warn_incomplete <- function(res) {
#   # if (dbHasCompleted(res)) return()

#   # is_before_first <- .jcall(res@jr, "Z", "isBeforeFirst")
#   # print(is_before_first)

#   print("afterLast: ")
#   go_last <- .jcall(res@jr, "Z", "last")
#   print(go_last)
#   print("getRow: ")
#   n_rows <- .jcall(res@jr, "I", "getRow")
#   print(n_rows)
#   print("beforeFirst: ")
#   before_first <- .jcall(res@jr, "Z", "beforeFirst")
#   print(before_first)
#   # stop()
#   # rows <- formatC(dbGetRowCount(res), big.mark = ",")
#   # warning("Only first ", rows, " results retrieved. Use n = -1 to retrieve all.",
#   #   call. = FALSE)
# }

ImpalaDBQuery <- R6::R6Class("ImpalaDBQuery",
  private = list(
    .nrow = NULL,
    .vars = NULL
  ),
  public = list(
    con = NULL,
    sql = NULL,

    initialize = function(con, sql, vars) {
      self$con <- con
      self$sql <- sql
      private$.vars <- vars
    },

    print = function(...) {
      cat("<Query> ", self$sql, "\n", sep = "")
      print(self$con)
    },

    fetch = function(n = -1L) {
      res <- dbSendQuery(self$con, self$sql)
      # res_warn_incomplete(res)
      on.exit(dbClearResult(res))
      out <- fetch(res, n)
      out
    },

    fetch_paged = function(chunk_size = 1e4, callback) {
      qry <- dbSendQuery(self$con, self$sql)
      on.exit(dbClearResult(qry))

      repeat {
        chunk <- fetch(qry, chunk_size)
        if (nrow(chunk) == 0) { break }
        callback(chunk)
      }

      invisible(TRUE)
    },

    vars = function() {
      private$.vars
    },

    nrow = function() {
      if (!is.null(private$.nrow)) return(private$.nrow)
      private$.nrow <- db_query_rows(self$con, self$sql)
      private$.nrow
    },

    ncol = function() {
      length(self$vars())
    }
  )
)

#' @export
query.ImpalaDBConnection <- function(con, sql, .vars) {
  assert_that(is.string(sql))
  ImpalaDBQuery$new(con, sql(sql), .vars)
}

#' @export
sql_escape_ident.ImpalaDBConnection <- function(con, x) {
  sql_quote(x, '`')
}

# impaladb_check_subquery <- function(sql) {
#   if (grepl("ORDER BY|LIMIT|OFFSET", as.character(sql), ignore.case=TRUE)) {
#     stop(from," contains ORDER BY, LIMIT or OFFSET keywords, which are not supported.")
#   }
# }


lahman_impaladb <- function(...) cache_lahman("impaladb", ...)



##############################################################
### Copied from dplyr - should be exported

cache <- new.env(parent = emptyenv())
is_cached <- function(name) exists(name, envir = cache)
set_cache <- function(name, value) {
#   message("Setting ", name, " in cache")
  assign(name, value, envir = cache)
  value
}
get_cache <- function(name) {
#   message("Getting ", name, " from cache")
  get(name, envir = cache)
}
lahman_tables <- function() {
  tables <- data(package = "Lahman")$results[, 3]
  tables[!grepl("Labels", tables)]
}


cache_lahman <- function(type, ...) {
  cache_name <- paste0("lahman_", type)
  if (is_cached(cache_name)) return(get_cache(cache_name))

  src <- src_impaladb("lahman", ...)
  tables <- setdiff(tolower(lahman_tables()), tolower(src_tbls(src)))
  orig_tables <- as.list(lahman_tables())
  names(orig_tables) <- tolower(lahman_tables())
  # do this to preserve case of names
  tables <- unlist(orig_tables[tables])

  # Create missing tables
  for(table in tables) {
    df <- getExportedValue("Lahman", table)
    message("Creating table: ", table)

    ids <- as.list(names(df)[grepl("ID$", names(df))])
    copy_to(src, df, table, indexes = ids, temporary = FALSE)
  }

  set_cache(cache_name, src)
}
