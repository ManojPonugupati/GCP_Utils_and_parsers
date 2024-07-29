import os
import requests
from google.oauth2 import service_account

# Set the path to your service account key file
SERVICE_ACCOUNT_FILE = 'path_to_your_service_account_key.json'

# Define the URL for downloading the billing report (hypothetical)
BILLING_REPORT_URL = 'https://cloudbilling.googleapis.com/v1/projects/{project_id}/billingInfo'

# Authenticate using the service account
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE,
    scopes=["https://www.googleapis.com/auth/cloud-platform"],
)

# Get the access token from the credentials
access_token = credentials.token

# Define the headers for the request
headers = {
    'Authorization': f'Bearer {access_token}',
    'Accept': 'application/json',
}

# Define the parameters for the billing report
params = {
    'invoiceMonth': '2023-06',  # Replace with the required invoice month
    'folders': 'all',
    'projects': 'all',
    'services': 'all',
    'skus': 'all',
}

# Send a request to download the billing report
response = requests.get(BILLING_REPORT_URL, headers=headers, params=params)

# Check if the request was successful
if response.status_code == 200:
    # Save the CSV file
    csv_file_path = 'billing_report.csv'
    with open(csv_file_path, 'wb') as file:
        file.write(response.content)
    print(f'Billing report saved to {csv_file_path}')
else:
    print(f'Failed to download billing report: {response.status_code} - {response.text}')
