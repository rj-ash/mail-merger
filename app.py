import streamlit as st
import pandas as pd
from data_loader import DataLoader
from mail_generation import MailGenerator
from email_sender import EmailSender
import asyncio
import json
import os
from datetime import datetime

# Set page config
st.set_page_config(
    page_title="Mail Merge Application",
    page_icon="✉️",
    layout="wide"
)

# Initialize session state for login
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'login_attempts' not in st.session_state:
    st.session_state.login_attempts = 0

# Login function
def check_credentials(username: str, password: str) -> bool:
    """Check if the provided credentials are valid."""
    return username == "rajs02073@gmail.com" and password == "rajsingh7222"

# Login page
if not st.session_state.authenticated:
    st.title("🔐 Login")
    
    # Create a centered container for the login form
    col1, col2, col3 = st.columns([1,2,1])
    with col2:
        with st.form("login_form"):
            st.subheader("Please Login")
            username = st.text_input("Email", type="default")
            password = st.text_input("Password", type="password")
            login_button = st.form_submit_button("Login")
            
            if login_button:
                if check_credentials(username, password):
                    st.session_state.authenticated = True
                    st.session_state.login_attempts = 0
                    st.success("Login successful!")
                    st.experimental_rerun()
                else:
                    st.session_state.login_attempts += 1
                    if st.session_state.login_attempts >= 3:
                        st.error("Too many failed attempts. Please try again later.")
                        st.stop()
                    else:
                        st.error(f"Invalid credentials. {3 - st.session_state.login_attempts} attempts remaining.")
    
    # Add some styling
    st.markdown("""
        <style>
        .stForm {
            background-color: #f0f2f6;
            padding: 2rem;
            border-radius: 10px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }
        </style>
    """, unsafe_allow_html=True)
    
    # Add a logout button in the sidebar if somehow authenticated
    if st.session_state.authenticated:
        if st.sidebar.button("Logout"):
            st.session_state.authenticated = False
            st.session_state.login_attempts = 0
            st.experimental_rerun()

# Main application
if st.session_state.authenticated:
    # Add logout button to sidebar
    if st.sidebar.button("Logout"):
        st.session_state.authenticated = False
        st.session_state.login_attempts = 0
        st.experimental_rerun()
    
    # Initialize other session state variables
    if 'data_loader' not in st.session_state:
        st.session_state.data_loader = DataLoader()
    if 'mail_generator' not in st.session_state:
        st.session_state.mail_generator = MailGenerator()
    if 'email_sender' not in st.session_state:
        st.session_state.email_sender = EmailSender()
    if 'df' not in st.session_state:
        st.session_state.df = None
    if 'generated_emails' not in st.session_state:
        st.session_state.generated_emails = None
    if 'sending_results' not in st.session_state:
        st.session_state.sending_results = None
    if 'show_preview' not in st.session_state:
        st.session_state.show_preview = False

    def download_csv(df: pd.DataFrame, filename: str):
        """Helper function to create a download button for DataFrames."""
        csv = df.to_csv(index=False)
        st.download_button(
            label=f"Download {filename} as CSV",
            data=csv,
            file_name=filename,
            mime="text/csv"
        )

    # Create tabs
    tab1, tab2, tab3 = st.tabs(["📊 Data Loading", "✍️ Email Generation", "📤 Email Sending"])

    with tab1:
        st.title("Data Loading")
        st.write("Upload your CSV or Excel file containing lead information.")
        
        # Show preview of current data if available
        if st.session_state.df is not None:
            st.subheader("Current Data Preview")
            st.dataframe(
                st.session_state.df,
                use_container_width=True
            )
            download_csv(st.session_state.df, "current_data.csv")
        
        uploaded_file = st.file_uploader(
            "Choose a file",
            type=['csv', 'xlsx', 'xls'],
            help="Upload a CSV or Excel file containing lead information. The file must have an 'email_id' column."
        )
        
        if uploaded_file is not None:
            try:
                # Save the uploaded file temporarily
                file_path = f"temp_{uploaded_file.name}"
                with open(file_path, "wb") as f:
                    f.write(uploaded_file.getvalue())
                
                # Load the file
                df = st.session_state.data_loader.load_file(file_path)
                st.session_state.df = df
                
                # Display the data
                st.subheader("Preview of Loaded Data")
                st.dataframe(
                    df,
                    use_container_width=True
                )
                
                # Download button for loaded data
                download_csv(df, f"loaded_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
                
                # Display data statistics
                st.subheader("Data Statistics")
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Leads", len(df))
                with col2:
                    st.metric("Columns", len(df.columns))
                with col3:
                    st.metric("Valid Emails", df['email_id'].notna().sum())
                
                # Display available columns
                st.subheader("Available Columns for Email Templates")
                st.write("You can use these column names as placeholders in your email templates using {column_name} format.")
                st.code(", ".join(df.columns), language="text")
                
                # Clean up temporary file
                os.remove(file_path)
                
                st.success("Data loaded successfully! Proceed to the Email Generation tab.")
                
            except Exception as e:
                st.error(f"Error loading file: {str(e)}")
                if os.path.exists(file_path):
                    os.remove(file_path)

    with tab2:
        st.title("Email Generation")
        
        if st.session_state.df is None:
            st.warning("Please load your data first in the Data Loading tab.")
        else:
            # Show preview of current data
            with st.expander("View Current Data", expanded=False):
                st.dataframe(st.session_state.df, use_container_width=True)
                download_csv(st.session_state.df, "current_data_for_templates.csv")
            
            # Email template input
            with st.form("email_template_form"):
                subject_template = st.text_input(
                    "Email Subject Template",
                    help="Use {column_name} for placeholders. Example: 'Hello {name}, interested in {company}'"
                )
                
                body_template = st.text_area(
                    "Email Body Template",
                    height=300,
                    help="Use {column_name} for placeholders. Example: 'Dear {name}, I hope this email finds you well...'"
                )
                
                preview_row = st.number_input(
                    "Preview Row Number",
                    min_value=0,
                    max_value=len(st.session_state.df) - 1,
                    value=0,
                    help="Select a row number to preview the generated email"
                )
                
                submitted = st.form_submit_button("Generate Emails")
            
            # Handle form submission outside the form
            if submitted and subject_template and body_template:
                try:
                    # Set templates
                    st.session_state.mail_generator.set_dataframe(st.session_state.df)
                    st.session_state.mail_generator.set_templates(subject_template, body_template)
                    
                    # Generate emails
                    generated_df = st.session_state.mail_generator.generate_emails()
                    st.session_state.generated_emails = generated_df
                    st.session_state.show_preview = True
                    
                    # Preview
                    preview = st.session_state.mail_generator.preview_email(preview_row)
                    
                    st.subheader("Email Preview")
                    st.write("**Subject:**")
                    st.write(preview['subject'])
                    st.write("**Body:**")
                    st.write(preview['body'])
                    
                    st.success("Emails generated successfully! Proceed to the Email Sending tab.")
                    
                except Exception as e:
                    st.error(f"Error generating emails: {str(e)}")
            
            # Show preview and download options outside the form
            if st.session_state.show_preview and st.session_state.generated_emails is not None:
                st.subheader("Preview of Generated Emails")
                preview_df = st.session_state.generated_emails[['email_id', 'email_subject', 'email_body']].head()
                st.dataframe(preview_df, use_container_width=True)
                download_csv(st.session_state.generated_emails, f"generated_emails_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
            
            # Show preview of previously generated emails if available
            elif st.session_state.generated_emails is not None:
                st.subheader("Previously Generated Emails")
                preview_df = st.session_state.generated_emails[['email_id', 'email_subject', 'email_body']].head()
                st.dataframe(preview_df, use_container_width=True)
                download_csv(st.session_state.generated_emails, "previously_generated_emails.csv")

    with tab3:
        st.title("Email Sending")
        
        if st.session_state.generated_emails is None:
            st.warning("Please generate emails first in the Email Generation tab.")
        else:
            # Show preview of emails to be sent
            with st.expander("Preview Emails to be Sent", expanded=False):
                preview_df = st.session_state.generated_emails[['email_id', 'email_subject', 'email_body']].head()
                st.dataframe(preview_df, use_container_width=True)
                download_csv(st.session_state.generated_emails, "emails_to_send.csv")
            
            # Display sending status
            status = st.session_state.email_sender.get_sending_status(st.session_state.generated_emails)
            
            st.subheader("Sending Status")
            col1, col2, col3, col4 = st.columns(4)
            with col1:
                st.metric("Total Leads", status['total_leads'])
            with col2:
                st.metric("Valid Emails", status['valid_emails'])
            with col3:
                st.metric("Valid Subjects", status['valid_subjects'])
            with col4:
                st.metric("Ready to Send", status['ready_to_send'])
            
            # Sending controls
            st.subheader("Send Emails")
            
            # Use a unique key for the send button
            send_button = st.button("Start Sending", key="send_emails_button", disabled=status['ready_to_send'] == 0)
            
            if send_button:
                try:
                    # Create a progress bar
                    progress_bar = st.progress(0)
                    status_text = st.empty()
                    
                    # Send emails
                    async def send_emails():
                        results = await st.session_state.email_sender.send_emails(st.session_state.generated_emails)
                        st.session_state.sending_results = results
                        return results
                    
                    # Run the async function
                    results = asyncio.run(send_emails())
                    
                    # Update progress
                    progress_bar.progress(1.0)
                    status_text.text("Sending completed!")
                    
                    # Display results
                    st.subheader("Sending Results")
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Total Emails", results['total_emails'])
                    with col2:
                        st.metric("Successful", results['successful'])
                    
                    if results['failed'] > 0:
                        st.warning(f"Failed to send {results['failed']} emails. Check the error log for details.")
                        with st.expander("Error Log"):
                            for error in results['errors']:
                                st.error(error)
                    
                    # Download results
                    results_file = f"email_results_{results['timestamp']}.json"
                    with open(os.path.join("email_results", results_file), 'r') as f:
                        results_json = f.read()
                    
                    st.download_button(
                        label="Download Sending Results (JSON)",
                        data=results_json,
                        file_name=results_file,
                        mime="application/json"
                    )
                    
                    # Create and download a summary CSV
                    summary_df = pd.DataFrame({
                        'timestamp': [results['timestamp']],
                        'total_emails': [results['total_emails']],
                        'successful': [results['successful']],
                        'failed': [results['failed']]
                    })
                    download_csv(summary_df, f"email_sending_summary_{results['timestamp']}.csv")
                    
                except Exception as e:
                    st.error(f"Error sending emails: {str(e)}")
                    progress_bar.empty()
                    status_text.empty()
            
            # Show previous sending results if available
            if st.session_state.sending_results is not None:
                st.subheader("Previous Sending Results")
                results = st.session_state.sending_results
                col1, col2 = st.columns(2)
                with col1:
                    st.metric("Total Emails", results['total_emails'])
                with col2:
                    st.metric("Successful", results['successful'])
                
                if results['failed'] > 0:
                    with st.expander("Previous Error Log"):
                        for error in results['errors']:
                            st.error(error)
                
                # Download previous results
                results_file = f"email_results_{results['timestamp']}.json"
                if os.path.exists(os.path.join("email_results", results_file)):
                    with open(os.path.join("email_results", results_file), 'r') as f:
                        results_json = f.read()
                    st.download_button(
                        label="Download Previous Results (JSON)",
                        data=results_json,
                        file_name=results_file,
                        mime="application/json"
                    )




