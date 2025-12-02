import time
import pandas as pd

from snowflake.snowpark import Session

STAGE = "@all_raw_db.visits.shoppertrak_visits_snowpark"
TARGET_TABLE = "RAW_VISITS_FRAHMAN_SNOWPARK"

# SNOWFLAKE ADVANTAGE: Schema detection
# SNOWFLAKE ADVANTAGE: Data ingestion with COPY
# SNOWFLAKE ADVANTAGE: Snowflake Tables (not file-based)

def validate_raw_table(session):
    table = session.table(TARGET_TABLE)
    row_count = table.count()
    table_schema = table.schema
    
    print(f"Total records in {TARGET_TABLE}: {row_count}")
    print(f"Schema of {TARGET_TABLE}: {table_schema}")
    table.describe().show()
    print("Sample data: \n", table.show(5))


def process_staged_files(session: Session):
    files = session.sql(f"LIST {STAGE}").collect()
    file_names = [f["name"] for f in files]

    for file in file_names:
        try:
            print(f"Processing file: {file}")
            relative_path = session.sql(
                f"SELECT GET_RELATIVE_PATH({STAGE}, '{file}')"
            ).collect()
            relative_path_str = relative_path[0][0]
            df = session.read.options({"skip_header": 1}).csv(
                f"{STAGE}/{relative_path_str}"
            )
            df.show()
            df.copy_into_table(TARGET_TABLE, validation_mode="CONTINUE")
        except Exception as e:
            print(f"Error processing file {file}: {e}")

    print("Done processing staged files!")


# For local debugging
if __name__ == "__main__":
    # Create a local Snowpark session
    # NOTE: One annoying snag is having to manually
    # dual authenticate while running this code... unsure how to fix
    with Session.builder.getOrCreate() as session:
        # NOTE: Snowpark cannot create resources (including tables)
        session.use_warehouse("LOADING_XS_WH")
        session.use_database("ALL_RAW_DB")
        session.use_role("ACCOUNTADMIN")
        process_staged_files(session)
        validate_raw_table(session)
