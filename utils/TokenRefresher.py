# Key vault 토큰 관리
"""
CLASS NAME: TokenRefresher  
DESCRIPTION: Databricks API를 통해 토큰 및 Secret Scope 관리, 만료 예정 토큰 자동 갱신 기능을 제공  
INHERITS: DatabricksAPIBase
"""

class TokenRefresher(DatabricksAPIBase):

    """
    METHOD NAME: listTokens  
    DESCRIPTION: 현재 사용자의 토큰 목록을 조회  
    RETURN VALUE: 토큰 정보 리스트 또는 빈 리스트
    """
    def listTokens(self):
        result = self._get("/api/2.0/token/list")
        return result.get("token_infos", []) if result else None

    """
    METHOD NAME: createToken  
    DESCRIPTION: 주어진 이름을 가진 새로운 Personal Access Token 생성  
    PARAMETER:  
    - token_name: 토큰 설명(comment)으로 사용할 이름  
    RETURN VALUE: 생성된 토큰 값(token_value) 또는 None
    """
    def createToken(self, token_name):
        data = {"comment": token_name}
        result = self._post("/api/2.0/token/create", data)
        return result.get("token_value") if result else None

    """
    METHOD NAME: deleteToken  
    DESCRIPTION: 토큰 ID를 이용해 토큰 삭제  
    PARAMETER:  
    - token_id: 삭제할 토큰의 ID  
    RETURN VALUE: 성공 시 True, 실패 시 None
    """
    def deleteToken(self, token_id):
        data = {"token_id": token_id}
        result = self._post("/api/2.0/token/delete", data)
        return True if result else None

    """
    METHOD NAME: listScopes  
    DESCRIPTION: 현재 존재하는 Secret Scope 목록을 반환  
    RETURN VALUE: Secret Scope 리스트 또는 None
    """
    def listScopes(self):
        result = self._get("/api/2.0/secrets/scopes/list")
        return result.get("scopes") if result else None

    """
    METHOD NAME: listSecrets  
    DESCRIPTION: 특정 Secret Scope 내의 키 목록을 반환  
    PARAMETER:  
    - scope_name: Secret Scope 이름  
    RETURN VALUE: Secret 키 리스트 또는 None
    """
    def listSecrets(self, scope_name):
        result = self._get("/api/2.0/secrets/list", params={"scope": scope_name})
        return result.get("secrets") if result else None

    """
    METHOD NAME: createScope  
    DESCRIPTION: 새로운 Secret Scope 생성  
    PARAMETER:  
    - scope_name: 생성할 Secret Scope 이름  
    RETURN VALUE: 성공 시 True, 실패 시 None
    """
    def createScope(self, scope_name):
        data = {"scope": scope_name,
                "initial_manager_principals": "MANAGE",
                "scope_backent_type": "DATABRICKS"
                }
        result = self._post("/api/2.0/secrets/scopes/create", data)
        return True if result else None

    """
    METHOD NAME: addSecret  
    DESCRIPTION: Secret Scope에 새로운 시크릿 키-값 쌍 추가  
    PARAMETER:  
    - scope: 대상 Secret Scope 이름  
    - key: 저장할 키 이름  
    - value: 저장할 문자열 값  
    RETURN VALUE: 성공 시 True, 실패 시 None
    """
    def addSecret(self, scope, key, value):
        data = {
            "scope": scope,
            "key": key,
            "string_value": value
        }
        result = self._post("/api/2.0/secrets/put", data)
        return True if result else None

    """
    METHOD NAME: convertUnixTimestamp  
    DESCRIPTION: Unix timestamp(ms) → datetime 객체로 변환  
    PARAMETER:  
    - unix_timestamp: 밀리초 단위의 Unix timestamp  
    RETURN VALUE: datetime 객체
    """
    def convertUnixTimestamp(self, unix_timestamp):
        return datetime.utcfromtimestamp(unix_timestamp / 1000)

    """
    METHOD NAME: getSecretInfo  
    DESCRIPTION: 모든 Databricks Secret Scope 및 관련 시크릿 키 정보를 수집  
    RETURN VALUE: scope-key 딕셔너리 리스트
    """
    def getSecretInfo(self):
        result = []
        scope_list = self.listScopes()
        if scope_list is None:
            return []

        token_info = self.listTokens()
        token_comments = [token['comment'] for token in token_info if isinstance(token, dict)] if token_info else []

        for scope in scope_list:
            if scope.get('backend_type') == "DATABRICKS":
                secrets = self.listSecrets(scope['name'])
                if secrets:
                    for secret in secrets:
                        result.append({"scope": scope['name'], "key": secret['key']})

        return result

    """
    METHOD NAME: refreshExpiringTokens  
    DESCRIPTION: 만료 예정 토큰을 자동으로 재생성 및 Secret Scope에 저장  
    RETURN VALUE: 갱신된 토큰의 scope-key 정보를 포함한 DataFrame
    """
    def refreshExpiringTokens(self):
        result = []
        now = datetime.now()
        secret_list = self.getSecretInfo()
        token_info = self.listTokens()

        token_dict = {token['comment']: token for token in token_info if isinstance(token, dict)} if token_info else {}

        for info in secret_list:
            key = info['key']
            scope = info['scope']
            renew_needed = False
            token = token_dict.get(key)

            if token:
                expiry = self.convertUnixTimestamp(token['expiry_time'])
                if expiry - now < timedelta(days=60):
                    renew_needed = True
            else:
                renew_needed = True

            if renew_needed:
                new_token = self.createToken(key)
                if new_token:
                    self.addSecret(scope, key, new_token)
                    result.append([scope, key])
                    if token:
                        self.deleteToken(token['token_id'])
                else:
                    print(f"토큰 생성 실패: {key}")

        schema = StructType([
            StructField("scope", StringType(), True),
            StructField("key", StringType(), True)
        ])
        df_result = spark.createDataFrame(result, schema)

        print(f"갱신 항목: {result}")
        display(df_result)

        return df_result
