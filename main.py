from sqlalchemy import create_engine, Column, Integer, String, UUID, MetaData, Table
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime

engine = create_engine('sqlite:///user.db', echo=True)


class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True, autoincrement=True)
    firstname = Column(String(100))
    lastname = Column(String(100))
    email = Column(String(100), unique=True)

    def __repr__(self):
        return f"<User(id={self.id}, firstname={self.firstname}, lastname={self.lastname}, email={self.email})>"

    @classmethod
    def add(cls, firstname, lastname, email):
        new_user = User(
            firstname=firstname,
            lastname=lastname,
            email=email
        )
        session.add(new_user)
        session.commit()


class Issue(Base):
    __tablename__ = "issues"
    key = Column(UUID, primary_key=True)
    summary = Column(String)
    assignee = Column(String)
    reporter = Column(String)
    priority = Column(String)
    status = Column(String)
    resolution = Column(String)
    created = Column(String)
    closed = Column(String)
    due_date = Column(String)
    version = Column(String)
    storypoints = Column(Integer)

    def __repr__(self):
        return (f"<Issue(key={self.key}, summary={self.summary}, assignee={self.assignee}, reporter={self.reporter}, "
                f"priority={self.priority}, status={self.status}, resolution={self.resolution}, created={self.created},"
                f"closed={self.closed}, due_date={self.due_date}, version={self.version}, storypoints={self.storypoints})>"
                )


Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

file_to_be_imported = "jira_issues_with_epic_hours.csv"
import_table = "jira"
FORMAT = "%Y-%m-%d"


# Data Analysis
# TODO Filter data as needed and plot it (generic)
# TODO Allow to apply single filter e.g. only issue-type "Bug"
# TODO Allow to apply multiple filters
# TODO Allow to sum values
# TODO Allow to count issues
# TODO Allow to calculate velocity
# TODO Run the import


# Read the CSV with Pandas
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
    print(f"{read_import_table_header.__name__} column_names: {column_names}")
    return column_names


# Compare the CSV Columns with the Existing Table Schema
def compare_csv_and_import_header():
    """Compares the CSV Header with the current import table and returns
    additional columns"""
    columns_to_be_dropped = [column for column in read_csv_header() if column not in read_import_table_header()]
    print(f"Columns to be dropped: {columns_to_be_dropped}")
    return columns_to_be_dropped


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


# def sum_filtered_values(filtered_df, column_to_sum):
#     column_sum = filtered_df[column_to_sum].sum()
#     print(f"Sum of {column_to_sum}: {column_sum}")


def get_filtered_issue_data(start_date, end_date, column_to_sum):
    df = pd.read_sql("jira", con=engine)
    df["Created"] = pd.to_datetime(df["Created"], format=FORMAT)
    start_date = datetime.strptime(start_date, FORMAT)
    end_date = datetime.strptime(end_date, FORMAT)

    mask_date_range = (df["Created"] >= start_date) & (df["Created"] <= end_date)
    filtered_df = df.loc[mask_date_range]
    print(filtered_df)

    # Sum the values in a specific column
    column_sum = filtered_df[column_to_sum].sum()
    print(f"Sum of {column_to_sum}: {column_sum}")


get_filtered_issue_data(start_date="2024-01-01", end_date="2024-07-31", column_to_sum="Story Points")


# Plot dataframe
def plot_transactions(dataframe):
    pass
