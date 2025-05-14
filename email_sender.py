import aiohttp
import asyncio
from typing import List, Dict, Any
import json
from datetime import datetime
import os
import pandas as pd

class EmailSender:
    def __init__(self, batch_size: int = 5):
        self.api_endpoint = "https://leadxmail-c5.onrender.com/send-emails"
        self.batch_size = batch_size
        self.results_dir = "email_results"
        
        # Create results directory if it doesn't exist
        if not os.path.exists(self.results_dir):
            os.makedirs(self.results_dir)

    async def send_email_batch(self, session: aiohttp.ClientSession, batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Send a batch of emails concurrently."""
        try:
            # Use SSL=False to disable certificate verification
            connector = aiohttp.TCPConnector(ssl=False)
            async with aiohttp.ClientSession(connector=connector) as session:
                async with session.post(self.api_endpoint, json=batch) as response:
                    if response.status == 200:
                        result = await response.json()
                        return result
                    else:
                        error_text = await response.text()
                        return [{"error": f"Failed to send batch: {error_text}", "status_code": response.status}]
        except Exception as e:
            return [{"error": str(e), "status_code": 500}]

    async def send_emails(self, email_payloads: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Send all emails in batches concurrently."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Create batches
        batches = [email_payloads[i:i + self.batch_size] 
                  for i in range(0, len(email_payloads), self.batch_size)]
        
        # Use SSL=False to disable certificate verification
        connector = aiohttp.TCPConnector(ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            # Create tasks for all batches
            tasks = [self.send_email_batch(session, batch) for batch in batches]
            
            # Execute all tasks concurrently
            await asyncio.gather(*tasks)
        
        # Return only total emails count
        return {
            "timestamp": timestamp,
            "total_emails": len(email_payloads)
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