# subsampleR

This repo provides `subsampleR` a flexible, modular pipeline for sub-sampling patient cohorts based on customizable criteria.
This package allows users to import data from various file formats, merge it with metadata, sub-sample datasets, perform distribution statistical tests, and visualize the results.

## Installation

To install the `subsampleR` package, you need to have R and the required libraries installed. You can directly clone the repository and use the following commands in R:

```R
# Install dependencies if you haven't already
install.packages(c("dplyr", "ggplot2", "readr", "readxl", "arrow", "tools"))

# Load the package (assuming the `subsampleR.R` file exists in your current directory)
source("subsampleR.R")
```

## Functions

### Data Import
```R
df <- step_import(file_path)
```

Imports data from various file formats (CSV, TSV, TXT, RDA, RDS, XLSX, Parquet).

### Merge Data with Metadata

```R
step_metadata(pipeline_object, metadata, cols = NULL)
```

Merges the input data with metadata based on the DAid column.

### Sub-Sample Data

```R
step_subsample(pipeline_object, n_samples, variable = NULL, ratio = NULL, seed = 123)
```

Sub-samples the data based on the specified number of samples or a ratio per category.

### Perform Kolmogorov-Smirnov Test

```R
step_ks_test(pipeline_object, population = "data", sample = "subsample_1", cols = NULL)
```

Performs the KS test between the population data and the sub-sampled data across specified columns.

### Visualize Distributions

```R
step_visualize(pipeline_object, population = "data", sample = "subsample_1", cols = NULL)
```

Visualizes the distribution of variables in both the population and sub-sampled data, generating histograms.

## Case Study

To see how the subsampleR package can be utilized in practice, you can download the case study from the following link:

[Case Study: SCAPIS Healthy CAC](https://github.com/HDA1472/subsampleR/blob/main/scapis_healthy_cac_casestudy.html)

## Contact

For any questions or further information, please contact us at konstantinos.antonopoulos@scilifelab.se. Contributions are welcome, please open an issue or submit a pull request!