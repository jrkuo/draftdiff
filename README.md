# draftdiff
# Creating Google Service Account
To create a service account that you can use to edit Google Sheets using Python, you'll need to follow these steps:

Create a Google Cloud Platform (GCP) project: Go to the Google Cloud Console, create a new project or select an existing one.

Enable the Google Sheets API: In the Cloud Console, navigate to the "APIs & Services" > "Library", search for "Google Sheets API", and enable it for your project.

Create a service account: Go to "APIs & Services" > "Credentials", then click on "Create credentials" and select "Service account". Follow the prompts to create a new service account. During this process, you'll be prompted to download a JSON key file. Save this file securely, as it will be used to authenticate your requests.

Share Google Sheets with the service account: Open your Google Sheet and share it with the email address associated with the service account. Grant appropriate permissions (edit or view) as needed.

# Downloading JSON key file
Go to the Google Cloud Console.
Select your project or create a new one.
In the left-hand side menu, click on "IAM & Admin" > "Service accounts".
Click on the "Create service account" button.
Enter a name and description for your service account, and then click on the "Create" button.
Assign the required roles to the service account. For Google Sheets API access, you'll typically need the "Editor" or "Viewer" role. You can always adjust the roles later if needed.
After creating the service account, you'll be prompted to grant the service account access to various Google APIs. You can skip this step if you plan to manually assign access later.
Once the service account is created, locate it in the list of service accounts and click on the three dots on the right-hand side, then select "Manage keys".
In the "Keys" tab, click on the "Add key" button and select "Create new key".
Choose the key type as JSON and click on the "Create" button. This will download the JSON key file to your computer.
This JSON key file contains the credentials for your service account and will be used to authenticate requests from your Python script to the Google Sheets API. Keep this file secure and don't share it publicly.