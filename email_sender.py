import aiohttp
import asyncio
from typing import List, Dict, Any, Optional
import json
from datetime import datetime
import os
import pandas as pd
import logging

class EmailSender:
    def __init__(self, batch_size: int = 5):
        self.api_endpoint = "https://smtp-rajs.onrender.com/send-emails"
        self.batch_size = batch_size
        self.results_dir = "email_results"
        self.logger = logging.getLogger(__name__)
        
        # Create results directory if it doesn't exist
        if not os.path.exists(self.results_dir):
            os.makedirs(self.results_dir)

    async def send_email_batch(self, session: aiohttp.ClientSession, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Send a batch of emails concurrently."""
        try:
            async with session.post(self.api_endpoint, json=batch) as response:
                if response.status == 200:
                    result = await response.json()
                    return result
                else:
                    error_text = await response.text()
                    self.logger.error(f"Failed to send batch: {error_text}")
                    return [{"error": f"Failed to send batch: {error_text}", "status_code": response.status}]
        except Exception as e:
            self.logger.error(f"Error sending batch: {str(e)}")
            return [{"error": str(e), "status_code": 500}]

    async def send_emails(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Send emails using the DataFrame containing generated emails.
        
        Args:
            df (pd.DataFrame): DataFrame containing 'email_id', 'email_subject', and 'email_body' columns
            
        Returns:
            Dict[str, Any]: Results of the email sending operation
        """
        if not all(col in df.columns for col in ['email_id', 'email_subject', 'email_body']):
            raise ValueError("DataFrame must contain 'email_id', 'email_subject', and 'email_body' columns")

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Prepare email payloads
        email_payloads = []
        for _, row in df.iterrows():
            if pd.isna(row['email_id']) or pd.isna(row['email_subject']) or pd.isna(row['email_body']):
                continue
                
            email_payloads.append({
                "email": [row['email_id']],  # API expects a list of emails
                "subject": row['email_subject'],
                "body": row['email_body']
            })
        
        # Create batches
        batches = [email_payloads[i:i + self.batch_size] 
                  for i in range(0, len(email_payloads), self.batch_size)]
        
        # Track results
        results = {
            "timestamp": timestamp,
            "total_emails": len(email_payloads),
            "successful": 0,
            "failed": 0,
            "errors": []
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                # Create tasks for all batches
                tasks = [self.send_email_batch(session, batch) for batch in batches]
                
                # Execute all tasks concurrently and collect results
                batch_results = await asyncio.gather(*tasks)
                
                # Process results
                for batch_result in batch_results:
                    for result in batch_result:
                        if "error" in result:
                            results["failed"] += 1
                            results["errors"].append(result["error"])
                        else:
                            results["successful"] += 1
                
                # Save results to file
                results_file = os.path.join(self.results_dir, f"email_results_{timestamp}.json")
                with open(results_file, 'w') as f:
                    json.dump(results, f, indent=2)
                
                return results
                
        except Exception as e:
            self.logger.error(f"Error in send_emails: {str(e)}")
            results["errors"].append(str(e))
            return results

    def get_sending_status(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Get statistics about the email sending status.
        
        Args:
            df (pd.DataFrame): DataFrame containing email data
            
        Returns:
            Dict[str, Any]: Statistics about the email sending status
        """
        if not all(col in df.columns for col in ['email_id', 'email_subject', 'email_body']):
            raise ValueError("DataFrame must contain 'email_id', 'email_subject', and 'email_body' columns")
            
        total = len(df)
        valid_emails = df['email_id'].notna().sum()
        valid_subjects = df['email_subject'].notna().sum()
        valid_bodies = df['email_body'].notna().sum()
        
        return {
            "total_leads": total,
            "valid_emails": valid_emails,
            "valid_subjects": valid_subjects,
            "valid_bodies": valid_bodies,
            "ready_to_send": min(valid_emails, valid_subjects, valid_bodies)
        }

def prepare_email_payloads(generated_emails: List[Dict[str, Any]], enriched_data: pd.DataFrame = None) -> List[Dict[str, Any]]:
    """Convert generated emails into the format required by the API."""
    payloads = []
    
    for result in generated_emails:
        if result["status"] == "success" and result["final_result"]:
            # Get lead_id and find email from enriched data
            lead_id = result.get("lead_id")
            if not lead_id or enriched_data is None:
                continue
                
            # Get email from enriched data
            lead_data = enriched_data[enriched_data['lead_id'] == lead_id]
            if lead_data.empty:
                continue
                
            email = lead_data['email'].iloc[0]
            subject = result["final_result"].get("subject", "")
            body = result["final_result"].get("body", "")
            
            # Skip if any required field is missing
            if not all([email, subject, body]):
                continue
                
            # Skip if email is 'N/A'
            if email == 'N/A':
                continue
                
            payloads.append({
                "email": [email],  # API expects a list of emails
                "subject": subject,
                "body": body
            })
    
    return payloads 