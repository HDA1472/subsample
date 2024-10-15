library(tools)
library(readr)
library(readxl)
library(arrow)
library(dplyr)


#' Import data from various file formats
#' 
#' This function imports data from various file formats, including CSV, TSV, TXT, RDA, RDS, XLSX, and Parquet.
#'
#' @param file_path Path to the file to import
#'
#' @return A pipeline object containing the imported data
#' @export
#'
#' @examples
#' df <- step_import("data/my_data.csv")
step_import <- function(file_path) {
  
  # Determine file extension from file path
  file_extension <- file_ext(file_path)
  
  df <- switch(tolower(file_extension),
               csv = read_csv(file_path),
               tsv = read_tsv(file_path),
               txt = read.table(file_path, header = TRUE, stringsAsFactors = FALSE),
               rda = { load(file_path); get(ls()[1]) },
               rds = readRDS(file_path),
               xlsx = read_excel(file_path, guess_max=10000000),
               parquet = read_parquet(file_path),
               stop("Unsupported file type: ", file_extension))
  
  df_out <- as_tibble(df)
  pipeline_object <- list(data = df_out)
  
  return(pipeline_object)
}


#' Merge data with metadata
#'
#' This function merges the input data with metadata based on the DAid column.
#' It also allows for selecting specific columns from the metadata to merge.
#' 
#' @param pipeline_object A pipeline object containing the data
#' @param metadata A tibble containing the metadata
#' @param cols A character vector of column names to select from the metadata
#'
#' @return A pipeline object containing the merged data and metadata
#' @export
#'
#' @examples
#' df_with_metadata <- step_metadata(df, metadata, cols = c("Sex", "Age", "Disease"))
step_metadata <- function(pipeline_object, metadata, cols = NULL) {
  
  df_out <- pipeline_object$data |> 
    left_join(metadata |> select(all_of(cols)), by = "DAid")
  
  pipeline_object$data <- df_out
  pipeline_object$metadata <- metadata
  return(pipeline_object)
}


#' Sub-sample data
#' 
#' This function sub-samples the data. The sub-sampling can be done based on the 
#' specified number of samples or a ratio per category.
#'
#' @param pipeline_object A pipeline object containing the data to be sub-sampled
#' @param n_samples The number of samples to select
#' @param variable The variable to use for sub-sampling (optional)
#' @param ratio The ratio of samples to select per category (optional)
#' @param seed The random seed for reproducibility (default = 123)
#'
#' @return
#' @export
#'
#' @examples
#' # Sub-sample 100 rows from the data
#' subsample <- step_import("data/my_data.csv") |> step_subsample(100)
#' 
#' # Sub-sample 100 rows based on the "Sex" variable
#' ## Define one ratio for all categories
#' subsample <- step_import("data/my_data.csv") |> step_subsample(100, "Sex", 0.5)
#' 
#' ## Define ratios for each category
#' subsample <- step_import("data/my_data.csv") |> step_subsample(100, "Sex", c("M" = 0.3, "F" = 0.7))
step_subsample <- function(pipeline_object, n_samples, variable = NULL, ratio = NULL, seed = 123) {
  
  # Check if the pipeline object contains already sampling steps
  existing_keys <- names(pipeline_object$subsample)
  if (length(existing_keys) == 0) {
    step_number <- 1  # Start with 1 if no keys exist
  } else {
    # Extract step numbers and find the next available number
    step_numbers <- as.numeric(gsub("subsample_", "", existing_keys))
    step_number <- max(step_numbers, na.rm = TRUE) + 1  # Increment the highest number by 1
  }
  key <- paste0("subsample_", step_number)
  
  # Checks for input param `variable`
  if (!is.null(variable) && !(variable %in% colnames(pipeline_object$data))) {
    stop(paste("The variable", variable, "does not exist in the data."))
  }
  
  
  if (!is.null(variable)) {
    # Checks for input param `ratio`
    unique_values <- unique(pipeline_object$data[[variable]])
    
    if (!is.numeric(ratio) && length(ratio) != length(unique_values)) {
      stop(paste("The length of the ratio must match the number of unique values in", variable))
    }
    
    if (is.numeric(ratio) && length(ratio) == 1) {
      # If a single ratio is provided, apply it to all categories equally
      ratio <- setNames(rep(ratio, length(unique_values)), unique_values)
    }
  }
  
  
  # Apply sub-sampling
  if (is.null(variable)) {
    df_subset <- pipeline_object$data |> sample_n(n_samples)
    df_subset <- as_tibble(df_subset)
    pipeline_object$subsample[[key]]$subset <- df_subset
  } else {
    ratio <- setNames(ratio, unique_values)
    n_per_category <- round(n_samples * ratio)
    
    # Initialize an empty dataframe to store the sampled data
    df_subset <- data.frame()
    
    # Loop over each category and sample the calculated number of samples
    set.seed(seed)
    for (category in unique_values) {
      category_data <- pipeline_object$data |> filter(!!sym(variable) == category)
      n_samples <- n_per_category[category]
      
      if (nrow(category_data) < n_samples) {
        # If there are fewer observations than requested, sample all rows
        message(paste("Category", category, "has fewer observations than requested. Sampling all rows."))
        sampled_category <- category_data
      } else {
        sampled_category <- category_data |> sample_n(n_samples)
      }
      
      df_subset <- bind_rows(df_subset, sampled_category)
    }
    df_subset <- as_tibble(df_subset)
    pipeline_object$subsample[[key]]$subset <- df_subset
    pipeline_object$subsample[[key]]$sampled_params <- list(variable = variable, 
                                                            ratio = ratio, 
                                                            seed = seed)
  }
  
  return(pipeline_object)
}


step_ks_test <- function(pipeline_object, cols = NULL) {
  
  initial_data <- pipeline_object$data
  subsample_data <- pipeline_object$subset
  ks_results <- list()

  # Loop through each specified column and perform the KS test
  for (col in cols) {
    # Check if the column exists in both datasets
    if (!(col %in% colnames(initial_data)) || !(col %in% colnames(subsample_data))) {
      warning(paste("Column", col, "not found in one of the datasets. Skipping."))
      next
    }

    initial_values <- initial_data[[col]]
    subsample_values <- subsample_data[[col]]
    
    # If the column is not numeric, convert it to numeric using factor
    if (!is.numeric(initial_values)) {
      # Convert both datasets to numeric using the same levels
      levels <- union(unique(initial_values), unique(subsample_values))
      initial_values <- as.numeric(factor(initial_values, levels = levels))
      subsample_values <- as.numeric(factor(subsample_values, levels = levels))
    }

    ks_res <- ks.test(initial_values, subsample_values)
    ks_results[[col]] <- list(
      D_statistic = ks_res$statistic[["D"]],    # D statistic
      p_value = ks_res$p.value,          # p-value
      null_hypothesis = ifelse(ks_res$p.value < 0.05, FALSE, TRUE)  # Null hypothesis
    )
  }
  
  ks_df <- do.call(rbind, lapply(ks_results, as.data.frame))
  pipeline_object$ks_test_results <- as_tibble(ks_df)
  
  return(pipeline_object)
}