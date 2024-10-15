library(tools)
library(readr)
library(readxl)
library(arrow)
library(dplyr)
library(ggplot2)

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
#' @return A pipeline object containing the sub-sampled data
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


#' Perform Kolmogorov-Smirnov (KS) test
#' 
#' This function performs the Kolmogorov-Smirnov (KS) test between the population data and the sub-sampled data.
#' The KS test is performed for each specified column, and the results are stored in the pipeline object.
#'
#' @param pipeline_object A pipeline object containing the data and sub-sampled data
#' @param population The population data (default = "data")
#' @param sample The sub-sampled data (default = "subsample_1")
#' @param cols A character vector of column names to perform the KS test
#'
#' @return A pipeline object containing the KS test results
#' @export
#'
#' @examples
#' # Perform KS test on the "Age" and "BMI" columns
#' ks_results <- step_import("data/my_data.csv") |> step_subsample(100) |> step_ks_test(cols = c("Age", "BMI"))
#' 
#' # Perform KS test on the "Age" and "BMI" columns using the sub-sampled data from step 2
#' ks_results <- step_import("data/my_data.csv") |> 
#'   step_subsample(100) |> 
#'   step_subsample(50) |> 
#'   step_ks_test(sample = "subsample_2", cols = c("Age", "BMI"))
#'   
#' # Perform KS test on the "Age" and "BMI" columns using the sub-sampled data from step 2 and population data from step 1
#' ks_results <- step_import("data/my_data.csv") |>
#'  step_subsample(100) |>
#'  step_subsample(50) |>
#'  step_ks_test(population = "subsample_1", sample = "subsample_2", cols = c("Age", "BMI"))
step_ks_test <- function(pipeline_object, population = "data", sample = "subsample_1", cols = NULL) {
  
  # Check if the pipeline object contains already KS steps
  existing_keys <- names(pipeline_object$ks_test_results)
  if (length(existing_keys) == 0) {
    step_number <- 1  # Start with 1 if no keys exist
  } else {
    # Extract step numbers and find the next available number
    step_numbers <- as.numeric(gsub("ks_test_", "", existing_keys))
    step_number <- max(step_numbers, na.rm = TRUE) + 1  # Increment the highest number by 1
  }
  key <- paste0("ks_test_", step_number)
  
  if (population != "data") {
    initial_data <- pipeline_object$subsample[[population]]$subset
  } else {
    initial_data <- pipeline_object[[population]]
  }
  subsample_data <- pipeline_object$subsample[[sample]]$subset
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
  pipeline_object$ks_test_results[[key]] <- as_tibble(ks_df)
  
  return(pipeline_object)
}


#' Visualize the distribution of variables
#' 
#' This function visualizes the distribution of variables in the population and sub-sampled data.
#'
#' @param pipeline_object A pipeline object containing the data and sub-sampled data
#' @param population The population data (default = "data")
#' @param sample The sub-sampled data (default = "subsample_1")
#' @param cols A character vector of column names to plot
#'
#' @return A pipeline object containing the plots
#' @export
#'
#' @examples
#' # Visualize the distribution of the "Age" and "BMI" columns
#' pipeline <- step_import("data/my_data.csv") |> step_subsample(100) |> step_visualize(cols = c("Age", "BMI"))
#' 
#' # Visualize the distribution of the "Age" and "BMI" columns using the sub-sampled data from step 2
#' pipeline <- step_import("data/my_data.csv") |>
#'  step_subsample(100) |>
#'  step_subsample(50) |>
#'  step_visualize(sample = "subsample_2", cols = c("Age", "BMI"))
#'  
#' # Visualize the distribution of the "Age" and "BMI" columns using the sub-sampled data from step 2 and population data from step 1
#' pipeline <- step_import("data/my_data.csv") |>
#'  step_subsample(100) |>
#'  step_subsample(50) |>
#'  step_visualize(population = "subsample_1", sample = "subsample_2", cols = c("Age", "BMI"))
step_visualize <- function(pipeline_object, population = "data", sample = "subsample_1", cols = NULL) {
  
  # Check if the pipeline object contains already KS steps
  existing_keys <- names(pipeline_object$histograms)
  if (length(existing_keys) == 0) {
    step_number <- 1  # Start with 1 if no keys exist
  } else {
    # Extract step numbers and find the next available number
    step_numbers <- as.numeric(gsub("histograms_", "", existing_keys))
    step_number <- max(step_numbers, na.rm = TRUE) + 1  # Increment the highest number by 1
  }
  key <- paste0("histograms_", step_number)
  
  if (population != "data") {
    initial_data <- pipeline_object$subsample[[population]]$subset
  } else {
    initial_data <- pipeline_object[[population]]
  }
  subsample_data <- pipeline_object$subsample[[sample]]$subset
  plots_list <- list()
  
  # Iterate through each variable
  for (col in cols) {
    # Check if the variable exists in both subsets
    if (!is.null(initial_data) && 
        !is.null(subsample_data)) {
      
      plot_data <- initial_data |> 
        select(all_of(col)) |> 
        mutate(Group = "Population") |> 
        rename(Value = col) %>%
        bind_rows(subsample_data |> 
                    select(all_of(col)) |> 
                    mutate(Group = "Subset") |> 
                    rename(Value = col))
      
      
      if (!is.numeric(plot_data$Value)) {
        p <- ggplot(plot_data, aes(x = Value, fill = Group)) +
          geom_histogram(stat = "count", position = "dodge", alpha = 0.5) +
          labs(title = paste("Distribution of", col),
               x = col,
               y = "Count") +
          theme_classic() +
          scale_fill_manual(values = c("Population" = "lightblue", "Subset" = "pink"))
      } else {
        p <- ggplot(plot_data, aes(x = Value, fill = Group)) +
          geom_histogram(position = "identity", alpha = 0.5, bins = 20) +
          labs(title = paste("Distribution of", col),
               x = col,
               y = "Frequency") +
          theme_classic() +
          scale_fill_manual(values = c("Population" = "lightblue", "Subset" = "pink"))
      }
      
      # Add the plot to the list with the variable name as the key
      plots_list[[col]] <- p
    } else {
      message(paste("Variable", col, "not found in both datasets. Skipping."))
    }
  }
  
  pipeline_object$histograms[[key]] <- plots_list
  return(pipeline_object)
}
