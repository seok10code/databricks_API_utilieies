# databricks_API_utilieies
📦 databricks_utils A lightweight and modular utility package for interacting with the Databricks REST API, designed to help you manage notebooks, users, secrets, and tokens efficiently.


✅ Features
🔐 TokenRefresher: Automatically refresh and rotate expiring Personal Access Tokens (PATs)

📓 NotebookOwnerManager: Backup and transfer ownership of notebooks with ease

🧾 SQLOwnerManager: Change ownership of SQL queries, dashboards, and alerts

📁 NotebookMigrator: Export, import, and migrate notebooks between folders



📁 Project Structure
databricks_utils/
├── base.py                   # Core request logic
├── notebook_owner_manager.py # Manages notebook ownership
├── sql_owner_manager.py      # Manages query/dashboard/alert ownership
├── notebook_migrator.py      # Manages notebook copying/migration
├── token_refresher.py        # Manages token rotation and secret updates
└── __init__.py               # Easy import entry point



🚀 Installation
git clone https://github.com/yourusername/databricks_utils.git
cd databricks_utils
# Use it directly or install with pip (optional)
pip install -e .



🛠️ Usage
from databricks_utils import NotebookOwnerManager, TokenRefresher

# Initialize API client
manager = NotebookOwnerManager("https://your-databricks-instance", token="abc123")

# Change notebook owner
manager.owner_change_method("/Users/alice/my_notebook")

# Refresh expiring tokens
refresher = TokenRefresher("https://your-databricks-instance", token="abc123")
refresher.renew_secrets()



📌 Requirements
Python 3.7+
requests
pyspark (for token/secrets and dataframe-related utilities)
