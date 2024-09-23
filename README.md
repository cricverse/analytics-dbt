# analytics-dbt

## Project Overview

The `analytics-dbt` project is designed to streamline and enhance the data transformation process using dbt (data build tool). This project aims to provide a robust and scalable analytics layer for your data warehouse, enabling efficient data modeling, transformation, and analysis.

## Project Structure

The project is organized into several key directories and files:

- `models/`: Contains the dbt models for transforming raw data into meaningful insights.
- `analyses/`: Stores SQL queries and analyses that can be run against the transformed data.
- `tests/`: Includes tests to ensure data quality and integrity.
- `seeds/`: Contains CSV files that can be loaded into the data warehouse as tables.
- `macros/`: Stores reusable SQL snippets and macros.
- `snapshots/`: Contains snapshot definitions for capturing historical data changes.

## Configuration

The project configuration is defined in the `dbt_project.yml` file. Key configurations include:

- `name`: The name of the project.
- `version`: The version of the project.
- `profile`: The dbt profile to use for this project.
- `model-paths`, `analysis-paths`, `test-paths`, `seed-paths`, `macro-paths`, `snapshot-paths`: Directories where dbt should look for different types of files.
- `clean-targets`: Directories to be removed by `dbt clean`.

## Profiles

The dbt profiles are defined in the `profiles.yml` file located in the user's `.dbt` directory. This file contains the connection details for the data warehouse, including:

- `dbname`: The name of the database.
- `host`: The host address of the database.
- `pass`: The password for the database user.
- `port`: The port number for the database connection.
- `schema`: The default schema to use.
- `threads`: The number of threads to use for parallel execution.
- `type`: The type of database (e.g., postgres).
- `user`: The username for the database connection.

## Packages

The project uses the `dbt_utils` package to leverage additional dbt functionalities. The package details are specified in the `packages.yml` and `package-lock.yml` files.

## Database Management

The `DatabaseManager` class in the `ingestion/lambda_function.py` file handles all database-related operations, including:

- Creating and managing database connections.
- Executing SQL statements to create and manage tables.
- Inserting data into the database.

## Getting Started

To get started with the `analytics-dbt` project, follow these steps:

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/analytics-dbt.git
   cd analytics-dbt
   ```

2. Install the required packages:

   ```bash
   dbt deps
   ```

3. Set up your dbt profile in the `profiles.yml` file.

4. Run the dbt models:

   ```bash
   dbt run
   ```

5. Test the models:

   ```bash
   dbt test
   ```

6. Generate documentation:

   ```bash
   dbt docs generate
   ```

7. Serve the documentation:
   ```bash
   dbt docs serve
   ```

## Contributing

We welcome contributions to the `analytics-dbt` project. Please open an issue or submit a pull request with your changes.

## License

This project is licensed under the MIT License. See the `LICENSE` file for more details.
