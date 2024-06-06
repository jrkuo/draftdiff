python -m pip install virtualenv
python -m virtualenv venv3.11
python -m pip install poetry
python -m poetry add beautifulsoup4 pandas loguru tqdm requests google-api-python-client oauth2client google-auth gspread
python -m pip install -e .