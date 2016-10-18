\name{clickhouse}
\alias{clickhouse}

\title{
  DBI database connector for external clickhouse databases
}
\description{
  \code{clickhouse} creates a new DBI driver that can be used to connect and interact with external clickhouse databases.
}
\usage{
  clickhouse()
}
\value{
  Returns a driver object that can be used in calls to
  \code{\link[DBI]{dbConnect}} with an external clickhouse database.
}
\details{
  The \code{clickhouse()} function creates the R object which can be used to a
  call \code{\link[DBI]{dbConnect}} which actually creates the connection. 
  Since it has no parameters, it is most commonly used inline with the \code{\link[DBI]{dbConnect}} call.
 
}
\seealso{
  \code{\link[DBI]{dbConnect}} for documentation how to invoke the driver  
}
\examples{
\dontrun{
library(DBI)
con <- dbConnect(clickhouse::clickhouse())
dbWriteTable(con, "mtcars", mtcars)
dbListTables(con)
dbGetQuery(con, "SELECT COUNT(*) FROM mtcars")
d <- dbReadTable(con, "mtcars")
dbDisconnect(con)
}}
\keyword{interface}
