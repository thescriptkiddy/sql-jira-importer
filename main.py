from sqlalchemy import create_engine, Column, Integer, String, UUID, MetaData, Table
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from uuid import uuid4
import csv
import pandas as pd

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

# Data Analysis
# TODO Read sql data into panda dataframe
# TODO Filter data as needed and plot it (generic)
# TODO Allow to apply single filter e.g. only issue-type "Bug"
# TODO Allow to apply multiple filters
# TODO Allow to sum values
# TODO Allow to count issues
# TODO Allow to calculate velocity


# Read the CSV with Pandas
def read_csv_header():
    list_of_columns = list(pd.read_csv(file_to_be_imported, nrows=0).columns)
    print(f"List of Columns: {list_of_columns}")
    return list_of_columns


def read_import_table_header():
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


# Alter the Existing Table Schema to Add New Columns (optional for later)
def adjust_database_schema():
    """Alters the import table if columns are missing"""
    pass
    # Alter database (extend) it with additional columns


# Insert the Data into the Updated Table
# TODO Create new columns in database if needed
# TODO Check if columns have been created successfully
# TODO Run the import
def direct_import_csv():
    """Direct import into a table, no validation"""
    # Read CSV
    df = pd.read_csv(file_to_be_imported)
    # Find additional columns
    additional_columns = compare_csv_and_import_header()
    cleaned_df = df.drop(additional_columns, axis=1)
    cleaned_df.to_sql(name=import_table, con=engine, if_exists="append")


direct_import_csv()
