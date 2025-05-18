import pandas as pd
from typing import Union, Optional
import os

class DataLoader:
    def __init__(self):
        self.df: Optional[pd.DataFrame] = None
        self.file_path: Optional[str] = None
        self.file_type: Optional[str] = None

    def load_file(self, file_path: str) -> pd.DataFrame:
        """
        Load a CSV or Excel file into a pandas DataFrame.
        
        Args:
            file_path (str): Path to the CSV or Excel file
            
        Returns:
            pd.DataFrame: Loaded DataFrame
            
        Raises:
            ValueError: If file type is not supported or file doesn't exist
        """
        if not os.path.exists(file_path):
            raise ValueError(f"File not found: {file_path}")
            
        self.file_path = file_path
        self.file_type = os.path.splitext(file_path)[1].lower()
        
        try:
            if self.file_type == '.csv':
                self.df = pd.read_csv(file_path)
            elif self.file_type in ['.xlsx', '.xls']:
                self.df = pd.read_excel(file_path)
            else:
                raise ValueError(f"Unsupported file type: {self.file_type}. Please use CSV or Excel files.")
            
            # Validate required columns
            required_columns = ['email_id']  # Add more required columns if needed
            missing_columns = [col for col in required_columns if col not in self.df.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
            
            return self.df
            
        except Exception as e:
            raise ValueError(f"Error loading file: {str(e)}")
    
    def get_dataframe(self) -> Optional[pd.DataFrame]:
        """
        Get the current DataFrame.
        
        Returns:
            Optional[pd.DataFrame]: The loaded DataFrame or None if no file is loaded
        """
        return self.df
    
    def get_columns(self) -> list:
        """
        Get the list of columns in the current DataFrame.
        
        Returns:
            list: List of column names
        """
        if self.df is None:
            return []
        return list(self.df.columns)
    
    def validate_data(self) -> bool:
        """
        Validate the loaded data for email sending requirements.
        
        Returns:
            bool: True if data is valid, False otherwise
        """
        if self.df is None:
            return False
            
        # Check for required columns
        if 'email_id' not in self.df.columns:
            return False
            
        # Check for empty email addresses
        if self.df['email_id'].isnull().any():
            return False
            
        # Check for valid email format (basic validation)
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not self.df['email_id'].str.match(email_pattern).all():
            return False
            
        return True
