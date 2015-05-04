# dplyrimpaladb

dplyrimpaladb is a Database connector for ImpalaDB for dplyr the next iteration of plyr (from Hadley Wickham), focussed on tools for working with data frames (hence the `d` in the name).


## Installing dependencies

To be able to write tables to ImpalaDB you will need HDFS support provided by rhdfs - follow th installation instructions from https://github.com/RevolutionAnalytics/RHadoop/wiki.

The interface to ImpalaDB is driven by Hive JDBC.  This will require the driver to be installed, and one of the easiest ways to achieve this is by following the installation instructions provided by Cloudera here: http://cloudera.com/content/cloudera-content/cloudera-docs/Impala/latest/Installing-and-Using-Impala/ciiu_noncm_installation.html

You should only need to install the impala and impala-shell packages, unless you actually wnat to install a separate server.

## Install dependent R packages

install RJDBC and assertthat with:

* the latest released version from CRAN with

    ```R
    install.packages(c("RJDBC", "assertthat"))
    ````

(should nolonger be required) next install lazyeval with:

* the latest released version from CRAN with

    ```R
    devtools::install_github("hadley/lazyeval")
    ````

next install dplyr with:

* the latest released version from CRAN with

    ```R
    install.packages("dplyr")
    ````

* OR the latest development version from github with

    ```R
    devtools::install_github("hadley/dplyr")
    ```

Finally install dplyrimpaladb from github:

    ```R
    devtools::install_github("piersharding/dplyrimpaladb")
    ```

To get started, read the notes below, then read the help(src_impaladb).

If you encounter a clear bug, please file a minimal reproducible example on [github](https://github.com/piersharding/impaladbdplyr/issues).

## `src_impaladb`

Connect to the Database:

```R
library(dplyrimpaladb)

# optionally set the class path for the ImpalaDB JDBC connector - alternatively use the CLASSPATH environment variable
options(dplyr.jdbc.classpath = "/usr/lib/impala")

# To connect to a database first create a src:
flights <- src_impaladb('flights', host="dragon.local.net")
cat(brief_desc(flights))

# Simple query:
all_airports <- tbl(flights, sql('SELECT DISTINCT `origin`, `dest` FROM ontime_parquet'))
head(all_airports)
```

See dplyr for many more examples.
