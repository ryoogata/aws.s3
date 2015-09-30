#' @title Retrieves an object from an S3 bucket
#' 
#' @param bucket A character string containing the name of the bucket, or an object of class \dQuote{s3_bucket}.
#' @param object A character string containing the name of an object, or an object of class \dQuote{s3_object}.
#' @param headers List of request headers for the REST call.
#' @param ... Additional arguments passed to \code{\link{s3HTTP}}.
#'
#' @return A raw object.
#' @references \href{http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html}{API Documentation}
#' @keywords object
#' @examples \dontrun{}
#' @export
getobject <- function(bucket, object, headers = list(), ...) {
    if (inherits(bucket, "s3_bucket"))
        bucket <- bucket$Name
    if (inherits(object, "s3_object"))
        object <- object$Key

    r <- s3HTTP(verb = "GET", 
                bucket = bucket,
                path = paste0("/", object),
                ...)
    if (inherits(r, "aws_error")) {
        return(r)
    } else {
        r
    }
}

print.s3_object <- function(x, ...){
    cat("Key:           ", x$Key, "\n")
    cat("Modified:      ", x$LastModified, "\n")
    cat("ETag:          ", x$ETag, "\n")
    cat("Size (kb):     ", x$Size, "\n")
    cat("Owner:         ", x$Owner$DisplayName, "\n")
    cat("Storage class: ", x$StorageClass, "\n")
    invisible(x)
}

#' @title Write an object locally 
#' 
#' @description Download an S3 object and save it locally
#'
#' @details This function is identical to \code{\link{getobject}}, except
#' in addition to returning the object as a raw vector within R, this function
#' writes the object to a connection, as specified by the \code{con} argument.
#'
#' @param bucket A character string containing the name of the bucket, or an object of class \dQuote{s3_bucket}.
#' @param object A character string containing the name of an object, or an object of class \dQuote{s3_object}.
#' @param con An R connection (e.g., a character string specifying a file path).
#' @param headers List of request headers for the REST call.
#' @param ... Additional arguments passed to \code{\link{s3HTTP}}.
#'
#' @return A raw object, invisibly.
#' @keywords object
#' @examples \dontrun{}
#' @export
write_object <- function(bucket, object, con, headers = list(), ...) {
    if (inherits(bucket, "s3_bucket"))
        bucket <- bucket$Name
    if (inherits(object, "s3_object"))
        object <- object$Key

    r <- getobject(bucket = bucket, object = object, headers = headers, ...)
    writeBin(r, con = con)
    if (inherits(r, "aws_error")) {
        return(r)
    } else {
        invisible(r)
    }
}

get_acl <- function(bucket, object, ...) {
    if (inherits(bucket, "s3_bucket"))
        bucket <- bucket$Name
    if (missing(object)) {
        r <- s3HTTP(verb = "GET", 
                    bucket = bucket,
                    path = "?acl",
                    ...)
        
        if (inherits(r, "aws_error")) {
            return(r)
        } else {
            structure(r, class = "s3_bucket")
        }
    } else {
        if (inherits(object, "s3_object"))
            object <- object$Key
        r <- s3HTTP(verb = "GET", 
                    path = "/object?acl",
                    ...)
        if (inherits(r, "aws_error")) {
            return(r)
        } else {
            structure(r, class = "s3_object")
        }
    }
}

#' @title Retrieves a Bencoded dictionary (BitTorrent) for an object from an S3 bucket.
#' 
#' @param bucket A character string containing the name of the bucket, or an object of class \dQuote{s3_bucket}.
#' @param object A character string containing the name of an object, or an object of class \dQuote{s3_object}.
#' @param ... additional arguments passed to \code{\link{s3HTTP}}.
#'
#' @return Something.
#' @references \href{http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGETtorrent.html}{API Documentation}
#' @keywords object
#' @examples \dontrun{}
#' @export

get_torrent <- function(bucket, object, ...) {
    if (inherits(bucket, "s3_bucket"))
        bucket <- bucket$Name
    if (inherits(object, "s3_object"))
        object <- object$Key

    r <- s3HTTP(verb = "GET", 
                bucket = bucket,
                path = paste0("/", object, "?torrent"),
                ...)
    if (inherits(r, "aws_error")) {
        return(r)
    } else {
        return(r)
    }
}


# HEAD

headobject <- function(bucket, object, ...) {
    if (inherits(bucket, "s3_bucket"))
        bucket <- bucket$Name
    if (inherits(object, "s3_object"))
        object <- object$Key

    r <- s3HTTP(verb = "HEAD", 
                bucket = bucket,
                path = paste0("/", object),
                ...)
    if (inherits(r, "aws_error")) {
        return(r)
    } else {
        structure(r, class = "HEAD")
    }
}


# OPTIONS

optsobject <- function(bucket, object, ...) {
    if (inherits(bucket, "s3_bucket"))
        bucket <- bucket$Name
    if (inherits(object, "s3_object"))
        object <- object$Key

    r <- s3HTTP(verb = "OPTIONS", 
                bucket = bucket,
                path = paste0("/", object),
                ...)
    if (inherits(r, "aws_error")) {
        return(r)
    } else {
        return(r)
    }
}


# POST

postobject <- function(bucket, object, ...) {
    if (inherits(bucket, "s3_bucket"))
        bucket <- bucket$Name
    if (inherits(object, "s3_object"))
        object <- object$Key

    r <- s3HTTP(verb = "POST", 
                bucket = bucket,
                path = paste0("/", object),
                ...)
    if (inherits(r, "aws_error")) {
        return(r)
    } else {
        structure(r, class = "s3_object")
    }
}


# PUT

#' @title Puts an object into an S3 bucket
#'
#' @param file A character string containing the filename (or full path) of 
#' the file you want to upload to S3.
#' @param bucket A character string containing the name of the bucket, or an object of class \dQuote{s3_bucket}.
#' @param object A character string containing the name of an object, or an object of class \dQuote{s3_object}.
#' have in S3 (i.e., its "object key"). If missing, the filename is used.
#' @param headers List of request headers for the REST call.   
#' @param ... additional arguments passed to \code{\link{s3HTTP}}
#'
#' @return If successful, \code{TRUE}, otherwise an aws_error object.
#' @references \href{http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html}{API Documentation}
#' @keywords object
#' @examples \dontrun{}
#' @export

putobject <- function(file, bucket, object, headers = list(), ...) {
    if (inherits(bucket, "s3_bucket"))
        bucket <- bucket$Name
    if (!missing(object) && inherits(object, "s3_object"))
        object <- object$Key
    if (missing(object)) {
        object <- basename(file)
    }

    r <- s3HTTP(verb = "PUT", 
                bucket = bucket,
                path = paste0("/", object),
                headers = c(headers, list(`Content-Length` = file.size(file))), 
                request_body = file,
                ...)
    if (inherits(r, "aws_error")) {
        return(r)
    } else {
        TRUE
    }
}

putobject_acl <- function(bucket, object, ...) {
    if (inherits(bucket, "s3_bucket"))
        bucket <- bucket$Name
    if (missing(object)) {
        r <- s3HTTP(verb = "PUT", 
                    bucket = bucket,
                    path = "?acl",
                    ...)
        if (inherits(r, "aws_error")) {
            return(r)
        } else {
            structure(r, class = "s3_bucket")
        }
    } else {
        if (inherits(object, "s3_object"))
            object <- object$Key
        r <- s3HTTP(verb = "PUT", 
                    bucket = bucket,
                    path = paste0("/", object),
                    ...)
        if (inherits(r, "aws_error")) {
            return(r)
        } else {
            structure(r, class = "s3_object")
        }
    }
}

#' @title Copy an object into another S3 bucket
#'
#' @param from_bucket A character string containing the name of the bucket, or an object of class \dQuote{s3_bucket}, which you want to copy from.
#' @param to_bucket A character string containing the name of the bucket, or an object of class \dQuote{s3_bucket}, which you want to copy into.
#' @param from_object A character string containing the name of an object, or an object of class \dQuote{s3_object}, which you want to copy.
#' @param to_object A character string containing the name of an object, or an object of class \dQuote{s3_object}, which specifies the name the object should have in the new bucket. By default, the object key for the original object is used.
#' @param headers List of request headers for the REST call.   
#' @param ... additional arguments passed to \code{\link{s3HTTP}}
#'
#' @return Something...
#' @references \href{http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectCOPY.html}{API Documentation}
#' @keywords object
#' @examples \dontrun{}
#' @export

copyobject <- function(from_object, to_object = from_object, from_bucket, to_bucket, headers = list(), ...) {
    if (inherits(from_bucket, "s3_bucket"))
        from_bucket <- from_bucket$Name
    if (inherits(to_bucket, "s3_bucket"))
        to_bucket <- to_bucket$Name
    if (inherits(from_object, "s3_object"))
        from_object <- from_object$Key
    if (inherits(to_object, "s3_object"))
        to_object <- to_object$Key
    r <- s3HTTP(verb = "PUT", 
                bucket = to_bucket,
                path = paste0("/", object),
                headers = c(headers, 
                            `x-amz-copy-source` = paste0("/",from_bucket,"/",from_object)), 
                ...)
    if (inherits(r, "aws_error")) {
        return(r)
    } else {
        return(r)
    }
}


# DELETE

#' @title Deletes an object from an S3 bucket.
#'
#' @param bucket A character string containing the name of the bucket, or an object of class \dQuote{s3_bucket}.
#' @param object A character string containing the name of an object, or an object of class \dQuote{s3_object}.
#' @param ... Additional arguments passed to \code{\link{s3HTTP}}
#'
#' @return TRUE if successful, aws_error details if not.
#' @references \href{http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectDELETE.html}{API Documentation}
#' @keywords object
#' @examples \dontrun{}
#' @export
deleteobject <- function(bucket, object, ...) {
    if (inherits(bucket, "s3_bucket"))
        bucket <- bucket$Name
    if (inherits(object, "s3_object"))
        object <- object$Key

    if (length(object) == 1) {
        r <- s3HTTP(verb = "DELETE", 
                    bucket = bucket,
                    path = paste0("/", object),
                    ...)
        if (inherits(r, "aws_error")) {
            return(r)
        } else {
            TRUE
        }
    } else {
      
      ## Looks like we would rather use the XML package to serialize this XML doc..
        b1 <- 
'<?xml version="1.0" encoding="UTF-8"?>
<Delete>
    <Quiet>true</Quiet>
    <Object>'
#         <Key>Key</Key>
# version not implemented yet:
#         <VersionId>VersionId</VersionId>
        b2 <- 
'    </Object>
    <Object>
         <Key>Key</Key>
    </Object>
    ...
</Delete>'
        tmpfile <- tempfile()
        on.exit(unlink(tmpfile))
        b <- writeLines(paste0(b1, paste0("<Key>",object,"</Key>"), b2), tmpfile)
        md <- base64enc::base64encode(digest::digest(file = tmpfile, raw = TRUE))
        r <- s3HTTP(verb = "POST", 
                    bucket = bucket,
                    path = "?delete",
                    body = tmpfile,
                    headers = list(`Content-Length` = file.size(tmpfile), 
                                   `Content-MD5` = md), 
                    ...)
        if (inherits(r, "aws_error")) {
            return(r)
        } else {
            return(structure(r, class = "s3_object"))
        }
    }
    return(r)
}
