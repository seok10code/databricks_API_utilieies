# 📦 databricks_utils

A lightweight and modular utility package for interacting with the Databricks REST API, designed to help you manage notebooks, users, secrets, and tokens efficiently.

---

## ✅ Features

- 🔐 **TokenRefresher**: Automatically refresh and rotate expiring Personal Access Tokens (PATs)
- 📓 **NotebookOwnerManager**: Backup and transfer ownership of notebooks with ease
- 🧾 **SQLOwnerManager**: Change ownership of SQL queries, dashboards, and alerts
- 📁 **NotebookMigrator**: Export, import, and migrate notebooks between folders

---

## 📁 Project Structure

```
databricks_utils/
├── TokenRefresher.py
├── NotebookOwnerManager.py
├── SQLOwnerManager.py
├── NotebookMigrator.py
├── __init__.py
```

---

## 🛠️ Usage

```python
from databricks_utils import NotebookOwnerManager, TokenRefresher

# Initialize with your Databricks instance and PAT token
manager = NotebookOwnerManager("https://your-databricks-instance", token="abc123")
manager.transfer_ownership("/Users/alice/my_notebook")

refresher = TokenRefresher("https://your-databricks-instance", token="abc123")
refresher.refresh_expiring_tokens()
```

---

## 📦 Installation

```bash
pip install -e .
```
or use locally:

```python
import sys
sys.path.append("/your/path/to/databricks_utils")
```

---

## 🔧 Requirements

- `requests`
- `datetime`

---

> 📢 This project is intended for internal automation and educational use.
