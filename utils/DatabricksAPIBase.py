class DatabricksAPIBase:
    """
    CONSTRUCTOR  
    DESCRIPTION: Databricks API를 호출할 때 사용되는 Databricks API 주소와 token을 저장하는 클래스  
    PARAMETER:  
    - databricks_instance: Databricks API 주소 (ex. https://adb-12345678901234567.8.azuredatabricks.net)
    - token: Databricks API Token  
    """
    def __init__(self, databricks_instance, token):
        self.instance = databricks_instance
        self.token = token


    """
    METHOD NAME: _headers 
    DESCRIPTION: Databricks API를 호출할 때 사용되는 header를 생성하는 메서드  
    PARAMETER:  
    - content_type: header의 Content-Type 값  
    RETURN VALUE: header dictionary  
    """
    def _headers(self, content_type="application/json"):
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": content_type
        }


    """
    METHOD NAME: _get  
    DESCRIPTION: Databricks API의 GET 요청을 처리하는 메서드  
    PARAMETER:  
    - endpoint: 호출할 API의 endpoint  
    - params: 요청에 사용할 query parameter dictionary (기본값: None)  
    RETURN VALUE: 요청 결과의 JSON 데이터 또는 오류 발생 시 None  
    """
    def _get(self, endpoint, params=None):
        url = f"{self.instance}{endpoint}"
        try:
            response = requests.get(url, headers=self._headers(), params=params)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"GET 오류: {e}")
            return None

    """
    METHOD NAME: _post  
    DESCRIPTION: Databricks API의 POST 요청을 처리하는 메서드  
    PARAMETER:  
    - endpoint: 호출할 API의 endpoint  
    - data: 요청에 사용할 payload (기본값: None)  
    RETURN VALUE: 요청 결과의 JSON 데이터 또는 오류 발생 시 None  
    """
    def _post(self, endpoint, data=None):
        url = f"{self.instance}{endpoint}"
        try:
            response = requests.post(url, headers=self._headers(), json=data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"POST 오류: {e}")
            return None


"""
CLASS NAME: NotebookOpsBase  
DESCRIPTION:  
Databricks Workspace에서 노트북을 다루는 기본 작업들을 모은 베이스 클래스입니다.  
디렉토리 생성, 노트북 Export/Import, 삭제, 목록 조회 등 공통 기능을 제공합니다.  
다른 클래스에서 상속하여 재사용됩니다.

INHERITS: DatabricksAPIBase
"""