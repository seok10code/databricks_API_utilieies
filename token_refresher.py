# Key vault 토큰 관리
from .base import DatabricksAPIBase

class TokenRefresher(DatabricksAPIBase):
    def list_tokens(self):
        result = self._get("/api/2.0/token/list")
        return result.get("token_infos", []) if result else None

    def create_token(self, token_name):
        data = {"comment": token_name}
        result = self._post("/api/2.0/token/create", data)
        return result.get("token_value") if result else None

    def delete_token(self, token_id):
        data = {"token_id": token_id}
        result = self._post("/api/2.0/token/delete", data)
        return True if result else None

    def list_scopes(self):
        result = self._get("/api/2.0/secrets/scopes/list")
        return result.get("scopes") if result else None

    def list_secrets(self, scope_name):
        result = self._get("/api/2.0/secrets/list", params={"scope": scope_name})
        return result.get("secrets") if result else None

    def add_secret(self, scope, key, value):
        data = {
            "scope": scope,
            "key": key,
            "string_value": value
        }
        result = self._post("/api/2.0/secrets/put", data)
        return True if result else None

    def convert_unix_timestamp(self, unix_timestamp):
        return datetime.utcfromtimestamp(unix_timestamp / 1000)

    def get_secret_info(self):
        result = []
        scope_list = self.list_scopes()
        if scope_list is None:
            return []

        token_info = self.list_tokens()
        token_comments = [token['comment'] for token in token_info if isinstance(token, dict)] if token_info else []

        for scope in scope_list:
            if scope.get('backend_type') == "DATABRICKS":
                secrets = self.list_secrets(scope['name'])
                if secrets:
                    for secret in secrets:
                        result.append({"scope": scope['name'], "key": secret['key']})

        return result

    def refresh_expiring_tokens(self):
        result = []
        now = datetime.now()
        secret_list = self.get_secret_info()
        token_info = self.list_tokens()

        token_dict = {token['comment']: token for token in token_info if isinstance(token, dict)} if token_info else {}

        for info in secret_list:
            key = info['key']
            scope = info['scope']
            renew_needed = False
            token = token_dict.get(key)

            if token:
                expiry = self.convert_unix_timestamp(token['expiry_time'])
                if expiry - now < timedelta(days=60):
                    renew_needed = True
            else:
                renew_needed = True

            if renew_needed:
                new_token = self.create_token(key)
                if new_token:
                    self.add_secret(scope, key, new_token)
                    result.append([scope, key])
                    if token:
                        self.delete_token(token['token_id'])
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
