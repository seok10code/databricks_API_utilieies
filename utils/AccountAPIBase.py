class AccountAPIBase(AppAPIBase):
    """
    CONSTRUCTOR  
    DESCRIPTION: Azure 인증 정보를 상속받고 Databricks 계정 및 인스턴스 정보를 초기화  
    PARAMETER:  
    - tenantId: Azure AD 테넌트 ID  
    - clientId: Azure AD 앱의 Client ID  
    - clientSecret: Azure AD 앱의 Client Secret  
    - objectId: Azure AD 앱의 Object ID  
    - databricksInstance: 호출할 Databricks API base URL  
    - account_id: Databricks 계정 ID
    """
    def __init__(self, tenantId, clientId, clientSecret, objectId, databricksInstance, account_id):
        super().__init__(tenantId, clientId, clientSecret, objectId)
        self.instance = databricksInstance
        self.account_id = account_id

    """
    METHOD NAME: _headers  
    DESCRIPTION: API 요청에 사용할 Authorization 및 Content-Type 헤더 생성  
    PARAMETER:  
    - content_type: 헤더에 설정할 Content-Type (기본값: application/json)  
    RETURN VALUE: 헤더 딕셔너리
    """
    def _headers(self, content_type="application/json"):
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": content_type
        }

    """
    METHOD NAME: _get  
    DESCRIPTION: GET 요청을 보내고 응답을 JSON으로 반환  
    PARAMETER:  
    - endpoint: 호출할 API endpoint  
    - params: 쿼리 파라미터 딕셔너리 (기본값: None)  
    RETURN VALUE: JSON 응답 또는 None
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
    DESCRIPTION: POST 요청을 보내고 응답을 JSON으로 반환  
    PARAMETER:  
    - endpoint: 호출할 API endpoint  
    - data: 전송할 JSON 데이터 (기본값: None)  
    RETURN VALUE: JSON 응답 또는 None
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
    METHOD NAME: _delete  
    DESCRIPTION: DELETE 요청을 보내고 응답을 JSON으로 반환  
    PARAMETER:  
    - endpoint: 호출할 API endpoint  
    RETURN VALUE: JSON 응답 또는 None
    """
    def _delete(self, endpoint):
        url = f"{self.instance}{endpoint}"
        try:
            response = requests.delete(url, headers=self._headers())
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"DELETE 오류: {e}")
            return None

    """
    METHOD NAME: _patch  
    DESCRIPTION: PATCH 요청을 보내고 응답을 JSON으로 반환  
    PARAMETER:  
    - endpoint: 호출할 API endpoint  
    - data: 전송할 JSON 데이터 (기본값: None)  
    RETURN VALUE: JSON 응답 또는 None
    """
    def _patch(self, endpoint, data=None):
        url = f"{self.instance}{endpoint}"
        try:
            response = requests.patch(url, headers=self._headers(), json=data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"PATCH 오류: {e}")
            return None


"""
CLASS NAME: AccountManager  
DESCRIPTION: Databricks 계정 사용자 및 그룹 관리 기능을 제공하는 클래스  
INHERITS: AccountAPIBase
"""