import streamlit as st
import pandas as pd
from people_search import get_people_search_results
from people_enrich import get_people_data
from mail_generation import EmailGenerationPipeline
from email_sender import EmailSender, prepare_email_payloads
import time
import asyncio
import json
import PyPDF2
import io
import os
import dotenv

# Load environment variables
dotenv.load_dotenv()

# Set page config
st.set_page_config(page_title="Apollo.io People Pipeline", layout="wide")

# Initialize session state for storing search results and enrichment data
if 'search_results' not in st.session_state:
    st.session_state.search_results = None
if 'enriched_data' not in st.session_state:
    st.session_state.enriched_data = None
if 'search_completed' not in st.session_state:
    st.session_state.search_completed = False
if 'enrichment_completed' not in st.session_state:
    st.session_state.enrichment_completed = False
if 'product_details' not in st.session_state:
    st.session_state.product_details = None
if 'mail_generation_completed' not in st.session_state:
    st.session_state.mail_generation_completed = False
if 'generated_emails' not in st.session_state:
    st.session_state.generated_emails = []
if 'email_sending_completed' not in st.session_state:
    st.session_state.email_sending_completed = False
if 'email_sending_results' not in st.session_state:
    st.session_state.email_sending_results = None

# Create tabs for different stages
tab1, tab2, tab3, tab4 = st.tabs(["People Search", "People Enrichment", "Mail Generation", "Send Emails"])

with tab1:
    st.title("Apollo.io People Search")
    st.write("Search for people using Apollo.io API")

    # Create input fields for search
    with st.form("search_form"):
        st.subheader("Search Criteria")
        titles_input = st.text_input(
            "Job Titles (comma-separated)",
            value="Partner, Investor",
            help="Enter job titles separated by commas"
        )
        
        # Add include similar titles checkbox
        include_similar_titles = st.checkbox(
            "Include Similar Titles",
            value=False,
            help="Include people with similar job titles in the search results"
        )
        
        locations_input = st.text_input(
            "Locations (comma-separated)",
            value="India",
            help="Enter locations separated by commas"
        )
        
        industries_input = st.text_input(
            "Industries (comma-separated)",
            value="Venture Capital & Private Equity",
            help="Enter industries separated by commas"
        )
        
        col1, col2 = st.columns(2)
        with col1:
            per_page = st.number_input("Results per page", min_value=1, max_value=100, value=5)
        with col2:
            page = st.number_input("Page number", min_value=1, value=1)
        
        submitted = st.form_submit_button("Search")

    if submitted:
        # Process inputs
        titles = [title.strip() for title in titles_input.split(",")]
        locations = [location.strip() for location in locations_input.split(",")]
        industries = [industry.strip() for industry in industries_input.split(",")]
        
        with st.spinner("Searching..."):
            results = get_people_search_results(
                person_titles=titles,
                include_similar_titles=include_similar_titles,
                person_locations=locations,
                company_locations=locations,
                company_industries=industries,
                per_page=per_page,
                page=page
            )
            
            if results:
                # Store results in session state
                st.session_state.search_results = results
                st.session_state.search_completed = True
                
                # Convert results to DataFrame for display
                df = pd.DataFrame(results)
                column_order = ['id', 'name', 'title', 'company', 'email', 'email_status', 
                              'linkedin_url', 'location', 'page_number']
                df = df[column_order]
                
                st.subheader("Search Results")
                st.dataframe(
                    df,
                    use_container_width=True,
                    column_config={
                        "id": st.column_config.TextColumn("Lead ID", width="medium"),
                        "name": st.column_config.TextColumn("Name", width="medium"),
                        "title": st.column_config.TextColumn("Title", width="medium"),
                        "company": st.column_config.TextColumn("Company", width="medium"),
                        "email": st.column_config.TextColumn("Email", width="medium"),
                        "email_status": st.column_config.TextColumn("Email Status", width="small"),
                        "linkedin_url": st.column_config.LinkColumn("LinkedIn", width="medium"),
                        "location": st.column_config.TextColumn("Location", width="medium"),
                        "page_number": st.column_config.NumberColumn("Page", width="small")
                    }
                )
                
                # Download button for search results
                csv = df.to_csv(index=False)
                st.download_button(
                    label="Download Search Results as CSV",
                    data=csv,
                    file_name="apollo_search_results.csv",
                    mime="text/csv"
                )
                
                st.write(f"Total results: {len(results)}")
                st.success("Search completed! Proceed to the Enrichment tab to enrich these profiles.")
            else:
                st.warning("No results found. Try adjusting your search criteria.")

with tab2:
    st.title("People Enrichment")
    
    if not st.session_state.search_completed:
        st.warning("Please complete the search first to get lead IDs for enrichment.")
    else:
        if not st.session_state.enrichment_completed:
            if st.button("Start Enrichment"):
                # Get all lead IDs from search results
                lead_ids = [result['id'] for result in st.session_state.search_results]
                
                # Process in batches of 10
                batch_size = 10
                enriched_data = []
                
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                for i in range(0, len(lead_ids), batch_size):
                    batch = lead_ids[i:i + batch_size]
                    status_text.text(f"Processing batch {i//batch_size + 1} of {(len(lead_ids) + batch_size - 1)//batch_size}")
                    
                    # Get enriched data for batch
                    batch_df = get_people_data(batch)
                    if not batch_df.empty:
                        enriched_data.append(batch_df)
                    
                    # Update progress
                    progress = min((i + batch_size) / len(lead_ids), 1.0)
                    progress_bar.progress(progress)
                    
                    # Add a small delay to avoid rate limiting
                    time.sleep(1)
                
                # Combine all batch results
                if enriched_data:
                    final_df = pd.concat(enriched_data, ignore_index=True)
                    st.session_state.enriched_data = final_df
                    st.session_state.enrichment_completed = True
                    
                    # Clear progress indicators
                    progress_bar.empty()
                    status_text.empty()
                    
                    st.success("Enrichment completed!")
                else:
                    st.error("No data was enriched. Please try again.")
        
        # Display enriched data if available
        if st.session_state.enrichment_completed and st.session_state.enriched_data is not None:
            st.subheader("Enriched Data")
            st.dataframe(
                st.session_state.enriched_data,
                use_container_width=True,
                column_config={
                    "name": st.column_config.TextColumn("Name", width="medium"),
                    "linkedin_url": st.column_config.LinkColumn("LinkedIn", width="medium"),
                    "title": st.column_config.TextColumn("Title", width="medium"),
                    "email_status": st.column_config.TextColumn("Email Status", width="small"),
                    "email": st.column_config.TextColumn("Email", width="medium"),
                    "organization": st.column_config.TextColumn("Organization", width="medium"),
                    "company_industry": st.column_config.TextColumn("Industry", width="medium"),
                    "company_keywords": st.column_config.TextColumn("Keywords", width="large"),
                    "company_website": st.column_config.LinkColumn("Website", width="medium"),
                    "company_linkedin": st.column_config.LinkColumn("Company LinkedIn", width="medium"),
                    "company_twitter": st.column_config.LinkColumn("Twitter", width="medium"),
                    "company_facebook": st.column_config.LinkColumn("Facebook", width="medium"),
                    "company_angellist": st.column_config.LinkColumn("AngelList", width="medium"),
                    "company_size": st.column_config.NumberColumn("Company Size", width="small"),
                    "company_founded_year": st.column_config.NumberColumn("Founded Year", width="small"),
                    "company_location": st.column_config.TextColumn("Location", width="medium"),
                    "education": st.column_config.TextColumn("Education", width="large"),
                    "experience": st.column_config.TextColumn("Experience", width="large")
                }
            )
            
            # Download button for enriched data
            csv = st.session_state.enriched_data.to_csv(index=False)
            st.download_button(
                label="Download Enriched Data as CSV",
                data=csv,
                file_name="apollo_enriched_data.csv",
                mime="text/csv"
            )

with tab3:
    st.title("Mail Generation")
    
    # Add file upload section for direct processing
    st.subheader("Upload Leads Data (Optional)")
    
    # Add template download button
    template_data = pd.DataFrame({
        'lead_id': ['example_id_1', 'example_id_2'],
        'name': ['John Doe', 'Jane Smith'],
        'title': ['CEO', 'CTO'],
        'organization': ['Tech Corp', 'Innovation Inc'],
        'headline': ['Technology Leader', 'Software Expert'],
        'education': ['MBA, Computer Science', 'PhD, Engineering'],
        'company_industry': ['Technology', 'Software'],
        'email': ['john@example.com', 'jane@example.com'],
        'linkedin_url': ['https://linkedin.com/in/johndoe', 'https://linkedin.com/in/janesmith'],
        'email_status': ['verified', 'verified'],
        'company_keywords': ['AI, ML', 'Cloud, DevOps'],
        'company_website': ['https://techcorp.com', 'https://innovationinc.com'],
        'company_linkedin': ['https://linkedin.com/company/techcorp', 'https://linkedin.com/company/innovationinc'],
        'company_twitter': ['https://twitter.com/techcorp', 'https://twitter.com/innovationinc'],
        'company_facebook': ['https://facebook.com/techcorp', 'https://facebook.com/innovationinc'],
        'company_angellist': ['https://angel.co/techcorp', 'https://angel.co/innovationinc'],
        'company_size': [100, 200],
        'company_founded_year': [2010, 2015],
        'company_location': ['San Francisco, CA, USA', 'New York, NY, USA'],
        'experience': ['CEO at Tech Corp', 'CTO at Innovation Inc']
    })
    csv = template_data.to_csv(index=False)
    st.download_button(
        label="Download Template CSV",
        data=csv,
        file_name="leads_template.csv",
        mime="text/csv",
        help="Download a template CSV file with the required columns"
    )
    
    uploaded_leads_file = st.file_uploader("Upload Leads Data (CSV/Excel)", type=['csv', 'xlsx', 'xls'])
    
    if uploaded_leads_file is not None:
        try:
            # Read the uploaded file
            if uploaded_leads_file.name.endswith('.csv'):
                leads_df = pd.read_csv(uploaded_leads_file)
            else:  # Excel file
                leads_df = pd.read_excel(uploaded_leads_file)
            
            # Validate required columns
            required_columns = ['lead_id', 'name', 'title', 'organization', 'headline', 'education', 'company_industry', 'email']
            missing_columns = [col for col in required_columns if col not in leads_df.columns]
            
            if missing_columns:
                st.error(f"Missing required columns: {', '.join(missing_columns)}")
            else:
                # Store the uploaded data in session state
                st.session_state.enriched_data = leads_df
                st.session_state.enrichment_completed = True
                st.success("Leads data uploaded successfully!")
                
                # Show preview of the data
                with st.expander("Preview Uploaded Data"):
                    st.dataframe(leads_df.head())
        except Exception as e:
            st.error(f"Error processing file: {str(e)}")
    
    if not st.session_state.enrichment_completed:
        st.warning("Please either complete the enrichment process or upload a leads data file to generate emails.")
    else:
        # Product Details Upload Section
        st.subheader("Product Information")
        uploaded_file = st.file_uploader("Upload Product Document (PDF)", type=['pdf'])
        
        if uploaded_file is not None:
            try:
                # Read PDF content
                pdf_reader = PyPDF2.PdfReader(io.BytesIO(uploaded_file.read()))
                product_text = ""
                for page in pdf_reader.pages:
                    product_text += page.extract_text()
                
                # Store product details in session state
                st.session_state.product_details = product_text
                st.success("Product document processed successfully!")
                
                # Show preview
                with st.expander("Preview Product Details"):
                    st.text_area("Extracted Text", product_text, height=200)
            except Exception as e:
                st.error(f"Error processing PDF: {str(e)}")
        
        # Mail Generation Section
        if st.session_state.product_details:
            # Add configuration options
            st.subheader("Email Generation Configuration")
            col1, col2, col3 = st.columns(3)
            with col1:
                batch_size = st.number_input("Batch Size", min_value=1, max_value=100, value=10)
            with col2:
                max_retries = st.number_input("Max Retries", min_value=1, max_value=5, value=3)
            with col3:
                retry_delay = st.number_input("Retry Delay (seconds)", min_value=1, max_value=30, value=5)
            
            # Add resume option if there are previous results
            resume_generation = False
            if st.session_state.generated_emails:
                resume_generation = st.checkbox("Resume previous generation", 
                    help="Continue from where the previous generation left off")
            
            if st.button("Generate Emails") or (resume_generation and st.button("Resume Generation")):
                # Initialize loading state
                loading_placeholder = st.empty()
                status_placeholder = st.empty()
                progress_placeholder = st.empty()
                
                try:
                    # Show initial loading state
                    with loading_placeholder.container():
                        st.spinner("Preparing email generation...")
                    
                    # Prepare payloads for mail generation
                    enriched_df = st.session_state.enriched_data
                    
                    # Update status
                    status_placeholder.text("Validating data...")
                    
                    # Validate required columns
                    required_columns = ['lead_id', 'name', 'title', 'organization', 'headline', 'education', 'company_industry']
                    missing_columns = [col for col in required_columns if col not in enriched_df.columns]
                    
                    if missing_columns:
                        st.error(f"Missing required columns in data: {', '.join(missing_columns)}")
                        st.stop()
                    
                    # Update status
                    status_placeholder.text("Cleaning and preparing data...")
                    
                    # Clean and prepare the data
                    enriched_df = enriched_df.fillna('N/A')  # Replace NaN with 'N/A'
                    
                    # Convert all string columns to string type and strip whitespace
                    string_columns = ['name', 'title', 'organization', 'headline', 'education', 'company_industry']
                    for col in string_columns:
                        if col in enriched_df.columns:
                            enriched_df[col] = enriched_df[col].astype(str).str.strip()
                    
                    # Update status
                    status_placeholder.text("Creating email payloads...")
                    
                    all_payloads = []
                    skipped_leads = []
                    
                    for _, row in enriched_df.iterrows():
                        try:
                            # Validate required fields
                            if not row['lead_id'] or row['lead_id'] == 'N/A':
                                skipped_leads.append(f"{row['name']} (Missing lead_id)")
                                continue
                                
                            if not row['name'] or row['name'] == 'N/A':
                                skipped_leads.append(f"Lead ID: {row['lead_id']} (Missing name)")
                                continue
                            
                            # Combine title, organization, and headline for experience
                            experience_parts = []
                            if row['title'] != 'N/A':
                                experience_parts.append(f"Title: {row['title']}")
                            if row['organization'] != 'N/A':
                                experience_parts.append(f"Organization: {row['organization']}")
                            if row['headline'] != 'N/A':
                                experience_parts.append(f"Headline: {row['headline']}")
                            combined_experience = ' | '.join(experience_parts) if experience_parts else 'N/A'
                            
                            # Prepare the payload with proper validation
                            payload = {
                                "lead": {
                                    "lead_id": str(row['lead_id']).strip(),
                                    "name": str(row['name']).strip(),
                                    "experience": combined_experience,
                                    "education": str(row['education']).strip(),
                                    "company": str(row['organization']).strip(),
                                    "company_overview": 'N/A',
                                    "company_industry": str(row['company_industry']).strip()
                                },
                                "product": {
                                    "details": st.session_state.product_details.strip()
                                }
                            }
                            
                            # Validate payload structure
                            if not all(payload['lead'].values()):
                                skipped_leads.append(f"{row['name']} (Empty required fields)")
                                continue
                                
                            all_payloads.append(payload)
                            
                        except Exception as e:
                            skipped_leads.append(f"{row.get('name', 'Unknown')} (Error: {str(e)})")
                            continue
                    
                    if skipped_leads:
                        st.warning(f"Skipped {len(skipped_leads)} leads due to missing or invalid data:")
                        with st.expander("View Skipped Leads"):
                            for lead in skipped_leads:
                                st.write(f"- {lead}")
                    
                    if not all_payloads:
                        st.error("No valid leads to process after validation. Please check your data.")
                        st.stop()
                    
                    # Update status
                    status_placeholder.text("Initializing email generation pipeline...")
                    
                    # Create and run pipeline with custom configuration
                    pipeline = EmailGenerationPipeline(
                        batch_size=batch_size,
                        max_concurrent=5,
                        max_retries=max_retries,
                        retry_delay=retry_delay,
                        timeout=60,  # Increased timeout
                        save_interval=50
                    )
                    
                    # Update status and show progress bar
                    status_placeholder.text("Generating emails...")
                    progress_bar = progress_placeholder.progress(0)
                    
                    # Create event loop for async execution
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    
                    try:
                        # Run the pipeline
                        loop.run_until_complete(pipeline.process_all_leads(all_payloads))
                        
                        # Update generated emails list
                        if resume_generation and st.session_state.generated_emails:
                            # Combine previous and new results
                            existing_results = {r["lead_id"]: r for r in st.session_state.generated_emails}
                            new_results = {r["lead_id"]: r for r in pipeline.all_results}
                            existing_results.update(new_results)
                            st.session_state.generated_emails = list(existing_results.values())
                        else:
                            st.session_state.generated_emails = pipeline.all_results
                            
                        st.session_state.mail_generation_completed = True
                        
                        # Clear loading indicators
                        loading_placeholder.empty()
                        status_placeholder.empty()
                        progress_placeholder.empty()
                        
                        # Show success message with statistics
                        successful = len([r for r in st.session_state.generated_emails if r["status"] == "success"])
                        failed = len([r for r in st.session_state.generated_emails if r["status"] == "failed"])
                        st.success(f"Email generation completed! Successful: {successful}, Failed: {failed}")
                        
                        # Add option to retry failed emails
                        if failed > 0:
                            if st.button("Retry Failed Emails"):
                                st.session_state.mail_generation_completed = False
                                st.experimental_rerun()
                                
                    except Exception as e:
                        st.error(f"Error during email generation: {str(e)}")
                        # Save current progress
                        if pipeline.all_results:
                            st.session_state.generated_emails = pipeline.all_results
                            st.warning("Progress has been saved. You can resume the generation later.")
                    finally:
                        loop.close()
                        # Clear any remaining loading indicators
                        loading_placeholder.empty()
                        status_placeholder.empty()
                        progress_placeholder.empty()
                        
                except Exception as e:
                    st.error(f"Error during preparation: {str(e)}")
                    # Clear any remaining loading indicators
                    loading_placeholder.empty()
                    status_placeholder.empty()
                    progress_placeholder.empty()
        
        # Display generated emails if available
        if st.session_state.mail_generation_completed and st.session_state.generated_emails:
            st.subheader("Generated Emails")
            
            # Convert results to DataFrame for display
            email_data = []
            enriched_df = st.session_state.enriched_data  # Get reference to enriched data
            
            for result in st.session_state.generated_emails:
                if result["status"] == "success" and result["final_result"]:
                    # Get email from enriched data using lead_id
                    lead_id = result["lead_id"]
                    email = enriched_df[enriched_df['lead_id'] == lead_id]['email'].iloc[0] if not enriched_df[enriched_df['lead_id'] == lead_id].empty else 'N/A'
                    
                    email_data.append({
                        "lead_id": result["lead_id"],
                        "name": result["lead_name"],
                        "organization": result["company"],
                        "email": email,
                        "subject": result["final_result"].get("subject", "N/A"),
                        "body": result["final_result"].get("body", "N/A")
                    })
            
            if email_data:
                df = pd.DataFrame(email_data)
                st.dataframe(
                    df,
                    use_container_width=True,
                    column_config={
                        "lead_id": st.column_config.TextColumn("Lead ID", width="medium"),
                        "name": st.column_config.TextColumn("Name", width="medium"),
                        "organization": st.column_config.TextColumn("Organization", width="medium"),
                        "email": st.column_config.TextColumn("Email", width="medium"),
                        "subject": st.column_config.TextColumn("Subject", width="large"),
                        "body": st.column_config.TextColumn("Body", width="xlarge")
                    }
                )
                
                # Download button for generated emails
                csv = df.to_csv(index=False)
                st.download_button(
                    label="Download Generated Emails as CSV",
                    data=csv,
                    file_name="generated_emails.csv",
                    mime="text/csv"
                )
            else:
                st.warning("No emails were successfully generated.")

with tab4:
    st.title("Send Emails")
    
    if not st.session_state.mail_generation_completed:
        st.warning("Please complete the mail generation step first.")
    else:
        if not st.session_state.email_sending_completed:
            st.subheader("Email Sending Configuration")
            
            batch_size = st.number_input(
                "Batch Size",
                min_value=1,
                max_value=10,
                value=5,
                help="Number of emails to send in each batch"
            )
            
            if st.button("Start Sending Emails"):
                if not st.session_state.generated_emails:
                    st.error("No emails have been generated. Please go back to the Mail Generation tab.")
                else:
                    # Prepare email payloads with enriched data
                    email_payloads = prepare_email_payloads(
                        st.session_state.generated_emails,
                        st.session_state.enriched_data
                    )
                    
                    if not email_payloads:
                        st.error("No valid email payloads to send.")
                    else:
                        # Initialize email sender
                        sender = EmailSender(batch_size=batch_size)
                        
                        # Create progress bar
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        # Send emails
                        status_text.text("Sending emails...")
                        
                        try:
                            # Run the async email sending process
                            results = asyncio.run(sender.send_emails(email_payloads))
                            
                            # Update session state
                            st.session_state.email_sending_completed = True
                            st.session_state.email_sending_results = results
                            
                            # Clear progress indicators
                            progress_bar.empty()
                            status_text.empty()
                            
                            st.success("Email sending completed!")
                        except Exception as e:
                            st.error(f"Error sending emails: {str(e)}")
        
        # Display results if available
        if st.session_state.email_sending_completed and st.session_state.email_sending_results:
            results = st.session_state.email_sending_results
            
            st.subheader("Email Sending Results")
            
            # Display summary
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Emails", results["total_emails"])
            with col2:
                st.metric("Successful Sends", results["successful_sends"])
            with col3:
                st.metric("Failed Sends", results["failed_sends"])
            
            # Display results file location
            st.info(f"Detailed results saved to: {results['results_file']}")
            
            # Add a button to view detailed results
            if st.button("View Detailed Results"):
                try:
                    with open(results["results_file"], 'r') as f:
                        detailed_results = json.load(f)
                        st.json(detailed_results)
                except Exception as e:
                    st.error(f"Error loading detailed results: {str(e)}")
            
            # Add a button to reset the email sending state
            if st.button("Reset Email Sending"):
                st.session_state.email_sending_completed = False
                st.session_state.email_sending_results = None
                st.experimental_rerun()

# Update pipeline information
with st.expander("Pipeline Information"):
    st.write("""
    This pipeline consists of three stages:
    
    1. **People Search**
       - Search for people using job titles, locations, and industries
       - Results include basic profile information
       - Download search results as CSV
    
    2. **People Enrichment**
       - Enrich the found profiles with additional data
       - Process lead IDs in batches of 10
       - Get detailed information including industry and keywords
       - Download enriched data as CSV
    
    3. **Mail Generation**
       - Upload product information document
       - Generate personalized emails for each lead
       - View and download generated emails
       - Process leads in batches with retry logic
    
    Note: Each stage must be completed in sequence. The enrichment and mail generation processes may take some time as they process profiles in batches.
    """)




