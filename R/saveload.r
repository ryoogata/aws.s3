#' @title Save to S3
#'
#' @description Save an R object to S3 ala \code{\link[base]{save}}.
#' 
#' @details Save an R object directly to an S3 bucket rather than a local directory.
#' 
#' @param ... Additional arguments passed to \code{\link{s3HTTP}}.
#' @param bucket A character string containing the name of the bucket, or an object of class \dQuote{s3_bucket}.
#' @param object A character string containing the name of an object, or an object of class \dQuote{s3_object}.
#' @param opts An optional list of options to be passed to \code{\link{postobject}}.
#'
#' @return If successful, \code{NULL}, else the contents of the API error.
#' 
#' @keywords bucket, object
#' @seealso \code{\link{putobject}}, \code{\link{s3load}}
#' @examples \dontrun{}
#' @export

s3save <- function(..., bucket, object, opts = list()) {
    if (inherits(bucket, "s3_bucket"))
        bucket <- bucket$Name
    tmp <- tempfile(fileext = ".Rdata")
    on.exit(unlink(tmp))
    save(..., file = tmp)
    r <- do.call(postobject, c(list(bucket = bucket, object = object), opts))
    if (inherits(r, "aws-error"))
        return(r)
    else
        return(invisible())
}

#' @title Load from S3
#'
#' @description Load an R object from S3 ala \code{\link[base]{load}}.
#' 
#' @details Load an R object directly from an S3 bucket rather than a local directory.
#' 
#' @param bucket A character string containing the name of the bucket, or an object of class \dQuote{s3_bucket}.
#' @param object A character string containing the name of an object, or an object of class \dQuote{s3_object}.
#' @param opts An optional list of options to be passed to \code{\link{getobject}}.
#' @param envir An R environment, by default the parent environment, into which the R 
#' object(s) should be loaded
#'
#' @return If successful, \code{NULL}, else the contents of the API error.
#' 
#' @keywords bucket, object
#' @seealso \code{\link{getobject}}, \code{\link{s3save}}
#' @examples \dontrun{}
#' @export

s3load <- function(bucket, object, opts, envir = parent.frame()) {
    if (inherits(bucket, "s3_bucket"))
        bucket <- bucket$Name
    tmp <- tempfile(fileext = ".Rdata")
    on.exit(unlink(tmp))
    r <- do.call(getobject, c(list(bucket = bucket, object = object), opts))
    if (inherits(r, "aws-error")) {
        return(r)
    } else {
        writeBin(httr::content(r, "raw"), con = tmp)
        load(tmp, envir = envir)
        return(invisible())
    }
}
