#' Load data from Google Cloud Storage
#' @inheritParams load_table_from_cs
#' @param billing project ID to use for billing
#' @param create_disposition behavior for table creation if the destination
#'   already exists. defaults to \code{"CREATE_IF_NEEDED"},
#'   the only other supported value is \code{"CREATE_NEVER"}; see
#'   \href{https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.createDisposition}{the API documentation}
#'   for more information
#' @param write_disposition behavior for writing data if the destination already
#'   exists. defaults to \code{"WRITE_APPEND"}, other possible values are
#'   \code{"WRITE_TRUNCATE"} and \code{"WRITE_EMPTY"}; see
#'   \href{https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.writeDisposition}{the API documentation}
#'   for more information
#' @family jobs
#' @export
insert_load_from_cs_job <- function(project, dataset, table, billing = project,
                            source_uris,
                            create_disposition = "CREATE_IF_NEEDED",
                            write_disposition = "WRITE_APPEND") {
  assert_that(is.string(project), is.string(dataset), is.string(table),
              is.string(billing), is.character(source_uris),
              is.string(create_disposition), is.string(write_disposition))

  url <- sprintf("projects/%s/jobs", project)

  body <- list(
    configuration = list(
      load = list(
        sourceFormat = "CSV",
        # this is the object created using the cs.R script
        sourceUris = source_uris,
        autodetect = TRUE,
        # the csv file has header that needs to be skipped
        skipLeadingRows = 1,
        destinationTable = list(
          projectId = project,
          datasetId = dataset,
          tableId = table
        ),
        createDisposition = create_disposition,
        writeDisposition = write_disposition
      )
    )
  )

  bq_post(url, body)
}

#' Load data from Google Cloud Storage to a table
#' @param project project containing this table
#' @param dataset dataset containing this table
#' @param table name of table to load data into
#' @param source_uris the fully-qualified URIs that point to your data in Google
#'   Cloud Storage; see
#'   \href{https://cloud.google.com/bigquery/docs/reference/v2/jobs#configuration.load.sourceUris}{the API documentation}
#'   for more information
#' @return information on the destination table for further consumption by the
#'   list_tabledata* functions
#' @export
load_table_from_cs <- function(project, dataset, table, source_uris, ...) {
  assert_that(is.string(project), is.string(dataset), is.string(table),
              is.character(source_uris))

  job <- insert_load_from_cs_job(project, dataset, table, source_uris = source_uris,
                         ...)

  job <- wait_for(job)

  job$configuration$load$destinationTable
}
