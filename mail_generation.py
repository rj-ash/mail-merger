import pandas as pd
from typing import Optional, Dict, List
import re

class MailGenerator:
    def __init__(self):
        self.df: Optional[pd.DataFrame] = None
        self.subject_template: Optional[str] = None
        self.body_template: Optional[str] = None
        self.placeholders: List[str] = []

    def set_dataframe(self, df: pd.DataFrame) -> None:
        """
        Set the DataFrame containing lead information.
        
        Args:
            df (pd.DataFrame): DataFrame containing lead information
        """
        self.df = df.copy()
        self._extract_placeholders()

    def _extract_placeholders(self) -> None:
        """Extract all possible placeholders from the DataFrame columns."""
        if self.df is not None:
            self.placeholders = list(self.df.columns)

    def set_templates(self, subject_template: str, body_template: str) -> None:
        """
        Set the email subject and body templates.
        
        Args:
            subject_template (str): Template for email subject
            body_template (str): Template for email body
        """
        self.subject_template = subject_template
        self.body_template = body_template

    def _validate_templates(self) -> bool:
        """
        Validate that all placeholders in templates exist in the DataFrame.
        
        Returns:
            bool: True if templates are valid, False otherwise
        """
        if not self.subject_template or not self.body_template:
            return False

        # Find all placeholders in templates
        pattern = r'\{([^}]+)\}'
        subject_placeholders = re.findall(pattern, self.subject_template)
        body_placeholders = re.findall(pattern, self.body_template)
        
        # Check if all placeholders exist in DataFrame columns
        all_placeholders = set(subject_placeholders + body_placeholders)
        missing_placeholders = [p for p in all_placeholders if p not in self.placeholders]
        
        return len(missing_placeholders) == 0

    def generate_emails(self) -> pd.DataFrame:
        """
        Generate personalized emails for all leads.
        
        Returns:
            pd.DataFrame: DataFrame with added 'email_subject' and 'email_body' columns
            
        Raises:
            ValueError: If templates are not set or invalid
        """
        if self.df is None:
            raise ValueError("No DataFrame set. Please set DataFrame first.")
            
        if not self._validate_templates():
            raise ValueError("Invalid templates. Please check that all placeholders exist in the DataFrame.")
            
        # Create copies of the templates for each row
        self.df['email_subject'] = self.df.apply(
            lambda row: self.subject_template.format(**row.to_dict()),
            axis=1
        )
        
        self.df['email_body'] = self.df.apply(
            lambda row: self.body_template.format(**row.to_dict()),
            axis=1
        )
        
        return self.df

    def get_available_placeholders(self) -> List[str]:
        """
        Get list of available placeholders from the DataFrame.
        
        Returns:
            List[str]: List of available placeholders
        """
        return self.placeholders

    def preview_email(self, row_index: int = 0) -> Dict[str, str]:
        """
        Preview the generated email for a specific row.
        
        Args:
            row_index (int): Index of the row to preview
            
        Returns:
            Dict[str, str]: Dictionary containing subject and body of the preview email
            
        Raises:
            ValueError: If row_index is invalid or templates are not set
        """
        if self.df is None or row_index >= len(self.df):
            raise ValueError("Invalid row index or DataFrame not set.")
            
        if not self.subject_template or not self.body_template:
            raise ValueError("Templates not set. Please set templates first.")
            
        row = self.df.iloc[row_index]
        return {
            'subject': self.subject_template.format(**row.to_dict()),
            'body': self.body_template.format(**row.to_dict())
        }
