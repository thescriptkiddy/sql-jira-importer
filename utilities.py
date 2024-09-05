from sqlalchemy.exc import SQLAlchemyError
import pandas as pd


def handle_exceptions(func):
    """Decorator to handle common exceptions."""
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except SQLAlchemyError as e:
            return f"Error: Database error - {str(e)}"
        except pd.errors.EmptyDataError:
            return "Error: File is empty"
        except pd.errors.ParserError:
            return "Error: Parsing error."
        except Exception as e:
            return f"Error: An unexpected error occurred - {str(e)}"
    return wrapper
