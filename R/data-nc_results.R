#' North Carolina local election results snapshot
#'
#' A cleaned snapshot of North Carolina local election results used by
#' DownBallotR. This dataset is primarily intended for fast access and
#' offline use; users can fetch newer data via \code{get_local_elections()}.
#'
#' @format A data frame with one row per candidate–contest and the following columns:
#' #' @format A data frame with columns:
#' \describe{
#'   \item{state}{Two-letter state abbreviation ("NC").}
#'   \item{year}{Election year.}
#'   \item{election_date}{Date of the election.}
#'   \item{election_type}{Election type (currently always "general").}
#'   \item{office}{Canonicalized office classification.}
#'   \item{office_raw}{Raw contest name from the source file.}
#'   \item{jurisdiction}{Jurisdiction name (e.g., county, municipality, district).}
#'   \item{jurisdiction_type}{Type of jurisdiction (e.g., county, school_district).}
#'   \item{district}{District identifier, if applicable.}
#'   \item{candidate}{Candidate or choice name.}
#'   \item{party}{Candidate party, if available.}
#'   \item{votes}{Total votes received.}
#'   \item{vote_share}{Share of votes in the contest.}
#'   \item{won}{Logical indicator for contest winner.}
#'   \item{incumbent}{Incumbency status (currently NA).}
#'   \item{source_url}{URL of the source election results file.}
#'   \item{retrieved_at}{Timestamp when data were retrieved.}
#' }
#'
#' @source North Carolina State Board of Elections
#'
#' @docType data
#' @name nc_results
NULL
