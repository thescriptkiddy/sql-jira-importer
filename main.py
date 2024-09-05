from sqlalchemy import create_engine, MetaData, Table, inspect
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from models.base import Base
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from utilities import handle_exceptions

engine = create_engine('sqlite:///user.db', echo=True)

Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

file_to_be_imported = "jira_issues_with_epic_hours.csv"
import_table = "jira"
FORMAT = "%Y-%m-%d"


def get_model_columns(model):
    """Returns the columns of a given model"""
    inspector = inspect(model)
    print([column.name for column in inspector.columns])
    return [column.name for column in inspector.columns]


def read_csv_header():
    """Reads the header of the given csv file"""
    try:
        list_of_columns = list(pd.read_csv(file_to_be_imported, nrows=0).columns)
        print(f"List of Columns: {list_of_columns}")

    except FileNotFoundError:
        return "Error: File not found"

    return list_of_columns


def read_import_table_header():
    """Reads the table header of the given SQL Table"""
    metadata = MetaData()
    table = Table(import_table, metadata, autoload_with=engine)
    column_names = table.columns.keys()
    # print(f"{read_import_table_header.__name__} column_names: {column_names}")
    return column_names


# Compare the CSV Columns with the Existing Table Schema
def compare_csv_and_import_header():
    """Compares the CSV Header with the current import table and returns
    additional columns"""
    missing_columns = [column for column in read_csv_header() if column not in read_import_table_header()]
    # print(f"Missing columns: {missing_columns}")
    return missing_columns


# Import Data into SQL Database
def direct_import_csv():
    """Imports the data into a sql table. If the table exists, it appends the data"""
    try:
        df = pd.read_csv(file_to_be_imported)
    except FileNotFoundError:
        return "Error: File not found."
    except PermissionError:
        return "Error: Permission denied."
    except pd.errors.EmptyDataError:
        return "Error: File is empty"
    except pd.errors.ParserError:
        return "Error: Parsing error."
    try:
        additional_columns = compare_csv_and_import_header()
        cleaned_df = df.drop(additional_columns, axis=1)
    except KeyError as e:
        return f"Error: Column issue - {str(e)}"
    try:
        cleaned_df.to_sql(name=import_table, con=engine, if_exists="append")
    except SQLAlchemyError as e:
        return f"Error: Database error - {str(e)}"

    return "Import completed successfully"


# Read data from SQL into Pandas Dataframe
def get_all_issues_data():
    df = pd.read_sql("jira", con=engine)
    print(df)
    return df


# Todo only allow to filter by existing columns
def sum_by_filter(filtered_df, column):
    try:
        sum_filter = filtered_df[column].sum()

    except pd.errors.EmptyDataError:
        return "Error: File is empty"
    except pd.errors.ParserError:
        return "Error: Parsing error."

    return f"Sum of {column}: {sum_filter}"


@handle_exceptions
def filter_dataframe(column=None, start_date=None, end_date=None, **filters):
    df = pd.read_sql("jira", con=engine)
    df[column] = pd.to_datetime(df[column], format=FORMAT)
    start_date = datetime.strptime(start_date, FORMAT)
    end_date = datetime.strptime(end_date, FORMAT)
    mask_date_range = (df[column] >= start_date) & (df[column] <= end_date)
    filtered_df = df.loc[mask_date_range]
    print(filtered_df)
    print(sum_by_filter(filtered_df, column="Story Points"))


# filter_dataframe(column="Created", start_date="2024-01-01", end_date="2024-01-31")


@handle_exceptions
def get_filtered_issue_data(column=None, start_date=None, end_date=None, column_to_sum=None, **filters):
    try:
        df = pd.read_sql("jira", con=engine)
        df[column] = pd.to_datetime(df[column], format=FORMAT)
        start_date = datetime.strptime(start_date, FORMAT)
        end_date = datetime.strptime(end_date, FORMAT)

        # Todo Create a separate function for dynamic filtering
        mask_date_range = (df[column] >= start_date) & (df[column] <= end_date)
        # mask_date_range = (df["Created"] >= start_date) & (df["Created"] <= end_date) & (df["Resolution"] == "Fixed")
        filtered_df = df.loc[mask_date_range]
        print(filtered_df)

        # Sum the values in a specific column
        column_sum = filtered_df[column_to_sum].sum()
        print(f"Sum of {column_to_sum}: {column_sum}")
    finally:
        pass

# get_filtered_issue_data(column="Created", start_date="2024-01-01", end_date="2024-07-31", column_to_sum="Story
# Points")
