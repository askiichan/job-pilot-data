**Objective:** To process CSV files from a date-named folder and convert them into structured JSON files.

**Instructions:**

1.  **Identify the target folder:** In the current directory, find the subfolder named with today's date in `YYYYMMDD` format (e.g., `20250826`).
2.  **Create an output folder:** In the current directory, create a new folder named `{target_folder_name}_processed` (e.g., `20250826_processed`).
3.  **Process each CSV file:** For every `.csv` file inside the target folder (`20250826`), perform the following:
    * Read the file content.
    * Extract: `job_title`, `company_name`, `post_date`, `job_description`, `job_requirement`.
    * If data is missing, use `null`.
    * Create a corresponding `.json` file in the `_processed` folder.
    * Write the extracted data in JSON format to the new file.