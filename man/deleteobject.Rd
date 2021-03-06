% Generated by roxygen2 (4.1.1): do not edit by hand
% Please edit documentation in R/OBJECT.r
\name{deleteobject}
\alias{deleteobject}
\title{Deletes an object from an S3 bucket.}
\usage{
deleteobject(bucket, object, ...)
}
\arguments{
\item{bucket}{A character string containing the name of the bucket you want to delete an object from.}

\item{object}{A character string containing the name of the object.}

\item{...}{Additional arguments passed to \code{\link{s3HTTP}}}
}
\value{
TRUE if successful, aws_error details if not.
}
\description{
Deletes an object from an S3 bucket.
}
\references{
\href{http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html}{API Documentation}
}

