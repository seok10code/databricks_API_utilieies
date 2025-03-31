import requests

class DatabricksAPIBase:
    def __init__(self, databricks_instance, token):
        self.instance = databricks_instance
        self.token = token

    def _headers(self, content_type="application/json"):
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": content_type
        }

    def _get(self, endpoint, params=None):
        url = f"{self.instance}{endpoint}"
        try:
            response = requests.get(url, headers=self._headers(), params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"GET 오류: {e}")
            return None

    def _post(self, endpoint, data=None):
        url = f"{self.instance}{endpoint}"
        try:
            response = requests.post(url, headers=self._headers(), json=data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"POST 오류: {e}")
            return None
