## Requirements

- Python 3.x
- ClickHouse server installed and running

## Setup

1. **Create and Activate Virtual Environment:**

    ```bash
    python3 -m venv venv
    source venv/bin/activate
    ```

2. **Install Required Python Packages:**

    ```bash
    pip install -r requirements.txt
    ```

3. **Ensure ClickHouse Server is Running:**

    Make sure your ClickHouse server is installed and running.

4. **Adjust Script Parameters (Optional):**

    Open the script and adjust the `clickhouse_url` and other connection parameters if needed.

## Usage

1. **Run the Script:**

    ```bash
    python main.py
    ```

2. The script will create the `myClickHouse` database, tables (`advertising_costs`, `user_activity`), and a materialized view (`user_activity_sessions`).

3. Data from CSV files in the `data/` directory is inserted into the `advertising_costs` table.

4. Manual data from the `dataForTask2/test_task_2.csv` file is inserted into the `user_activity` table.
