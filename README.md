# dplyrimpaladb

dplyrimpaladb is a Database connector for ImpalaDB for dplyr the next iteration of plyr (from Hadley Wickham), focussed on tools for working with data frames (hence the `d` in the name).


## Installing dependencies

To be able to write tables to ImpalaDB you will need HDFS support provided by rhdfs - follow the installation instructions from https://github.com/RevolutionAnalytics/RHadoop/wiki.

The interface to ImpalaDB is driven by a JDBC connection.  This will require the [driver jars](https://www.cloudera.com/downloads/connectors/impala/jdbc/2-5-29.html) to be installed from the Cloudera website. You should only need to install the impala and impala-shell packages if you actually want to run your own impala server.

You will also need java and curl-devel (or your operating system's equivalent) installed in order to build some of the required R packages.

## Install dependent R packages

install RJDBC and assertthat with:

* the latest released version from CRAN with

    ```R
    install.packages(c("RJDBC", "assertthat", "lazy", "devtools"))
    ```

next install dplyr with:

* the latest released version from CRAN with

    ```R
    install.packages("dplyr")
    ```

* OR the latest development version from github with

    ```R
    devtools::install_github("hadley/dplyr")
    ```

Finally install dplyrimpaladb from github:

```R
devtools::install_github("piersharding/dplyrimpaladb")
```

To get started, read the notes below.

If you encounter a clear bug, please file a minimal reproducible example on [github](https://github.com/piersharding/impaladbdplyr/issues).

## Usage

Load the library:

```R
library(dplyrimpaladb)
```


Then you can connect to an Impala schema.  There are a large number of parameters that src_impaladb accepts, and most of these are optional.  The opts parameter is passed through to the JDBC URI, the rest of the parameters are used in the loading of the Java classes and connecting to Impala.

```R
reference <- src_impaladb('reference',
                          host='impala.example.org',
                          user='my_username',
                          password='my_password',
                          driverclass="com.cloudera.impala.jdbc41.Driver",
                          classpath="/opt/jars/Cloudera_ImpalaJDBC41_2.5.29/",
                          scheme="impala",
                          jarfiles=c("hive_metastore.jar",
                                      "hive_service.jar",
                                      "ImpalaJDBC41.jar",
                                      "libfb303-0.9.0.jar",
                                      "libthrift-0.9.0.jar",
                                      "log4j-1.2.14.jar",
                                      "ql.jar",
                                      "slf4j-api-1.5.11.jar",
                                      "slf4j-log4j12-1.5.11.jar",
                                      "TCLIServiceClient.jar",
                                      "zookeeper-3.4.6.jar"
                                      ),
                          opts=list(auth="noSasl",
                                    AuthMech="4",
                                    SSLTrustStorePwd="changeit",
                                    SSLKeyStorePwd="changeit",
                                    SSLKeyStore="/etc/impala.jks",
                                    SSLTrustStore="/etc/impala.jks",
                                    uid="my_username",
                                    pwd="my_password") )
```

Finally, you can issue a simple query

```R
all_dates <- tbl(reference, sql('SELECT * FROM calendar'))
typeof(all_dates)
head(all_dates)
```