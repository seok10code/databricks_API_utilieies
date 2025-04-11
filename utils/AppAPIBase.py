# API BASE
"""
CLASS NAME: AppAPIBase  
DESCRIPTION: Azure AD App 등록 정보를 조회하고, Client Secret의 만료 여부를 확인하기 위한 API 호출 클래스  
"""

import base64
import requests
import json
import os
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime, timedelta, timezone
from dateutil import parser


class AppAPIBase():
    """
    CONSTRUCTOR  
    DESCRIPTION: Azure AD 토큰 발급에 필요한 정보와 App Object ID를 저장하고, 액세스 토큰을 즉시 획득  
    PARAMETER:  
    - tenant_id: Azure AD 테넌트 ID  
    - client_id: App 등록 시 발급받은 클라이언트 ID  
    - client_secret: App 등록 시 생성한 시크릿  
    - object_id: 애플리케이션 Object ID  
    - azure_instance: 로그인 요청에 사용할 Azure AD 도메인 (기본값: https://login.microsoftonline.com/)
    """
    def __init__(self, tenant_id, client_id, client_secret, object_id, azure_instance="https://login.microsoftonline.com/"):
        self.instance = azure_instance
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.object_id = object_id
        self.token = self.getAccessToken()

    """
    METHOD NAME: getAccessToken  
    DESCRIPTION: Microsoft OAuth2를 통해 client_credentials 방식으로 액세스 토큰을 획득  
    RETURN VALUE: 액세스 토큰 문자열 또는 None
    """
    def getAccessToken(self):
        url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
        data = {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "scope": "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default"
        }
        try:
            response = requests.post(url, data=data)
            response.raise_for_status()
            return response.json().get("access_token")
        except requests.exceptions.RequestException as e:
            print(f"Access Token 요청 실패: {e}")
            return None

    """
    METHOD NAME: getAppInfo  
    DESCRIPTION: Microsoft Graph API를 통해 App 등록 정보 조회  
    RETURN VALUE: App 정보가 담긴 JSON 응답
    """
    def getAppInfo(self):
        url = f"https://graph.microsoft.com/v1.0/applications/{self.object_id}"
        headers = {'Authorization': f'Bearer {self.token}'}
        response = requests.get(url, headers=headers)
        return response.json()

    """
    METHOD NAME: checkSecret  
    DESCRIPTION: App에 등록된 Client Secret의 남은 유효 기간을 출력  
    PRINT: Secret 이름 및 만료까지 남은 일수  
    RETURN VALUE: None
    """
    def checkSecret(self):
        now = datetime.now(timezone.utc)
        for secret in self.getAppInfo().get('passwordCredentials', []):
            display_name = secret.get('displayName', '(No Name)')
            end_time = parser.parse(secret['endDateTime'])
            days_left = (end_time - now).days
            print(f"Secret: {display_name} | 만료까지: {days_left}일")
