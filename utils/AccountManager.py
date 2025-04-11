class AccountManager(AccountAPIBase):
    """
    CONSTRUCTOR  
    DESCRIPTION: 상위 AccountAPIBase 초기화  
    PARAMETER:  
    - tenantId: Azure AD 테넌트 ID  
    - clientId: 앱 등록 Client ID  
    - clientSecret: 앱 등록 Client Secret  
    - objectId: 앱 Object ID  
    - databricksInstance: Databricks API 호출 base URL  
    - account_id: Databricks 계정 ID
    """
    def __init__(self, tenantId, clientId, clientSecret, objectId, databricksInstance, account_id):
        super().__init__(tenantId, clientId, clientSecret, objectId, databricksInstance, account_id)


    """
    METHOD NAME: getUserList  
    DESCRIPTION: 모든 사용자 정보를 페이지 단위로 불러와 병합  
    RETURN VALUE: 전체 사용자 리스트를 담은 Spark DataFrame
    """
    def getUserList(self):
        all_users = []
        start_index = 1
        while True:
            params = {
                "count": 100,
                "startIndex": start_index
            }
            response = self._get(f"/api/2.1/accounts/{self.account_id}/scim/v2/Users", params)
            resources = response.get("Resources", [])
            all_users.extend(resources)
            if len(resources) < 100:
                break
            start_index += 100

        return spark.createDataFrame(all_users)

    """
    METHOD NAME: getUserDetail  
    DESCRIPTION: 특정 사용자에 대한 상세 정보 조회  
    PARAMETER:  
    - user_id: 사용자 ID  
    RETURN VALUE: 사용자 상세 정보 (JSON)
    """
    def getUserDetail(self, user_id):
        return self._get(f"/api/2.1/accounts/{self.account_id}/scim/v2/Users/{user_id}")

    """
    METHOD NAME: createUser  
    DESCRIPTION: 새로운 사용자 생성  
    PARAMETER:  
    - userName: 로그인 이메일 (현재는 하드코딩된 값)  
    - givenName: 이름  
    - familyName: 성  
    RETURN VALUE: 생성된 사용자 정보 (JSON)
    """
    def createUser(self, userName, givenName, familyName):
        data = {
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:User"],
            "userName": userName,  # TODO: 변경 가능하게 수정 필요
            "name": {
                "givenName": f"{givenName}",
                "familyName": f"{familyName}"
            }
        }
        return self._post(f"/api/2.1/accounts/{self.account_id}/scim/v2/Users", data)

    """
    METHOD NAME: deleteUser  
    DESCRIPTION: 특정 사용자 삭제  
    PARAMETER:  
    - user_id: 사용자 ID  
    RETURN VALUE: 성공 시 응답 JSON 또는 None
    """
    def deleteUser(self, user_id):
        return self._delete(f"/api/2.1/accounts/{self.account_id}/scim/v2/Users/{user_id}")

    """
    METHOD NAME: getGroupList  
    DESCRIPTION: 그룹 이름 및 ID 목록 조회  
    RETURN VALUE: Spark DataFrame(group_name, group_id)
    """
    def getGroupList(self):
        return self._get(f"/api/2.1/accounts/{self.account_id}/scim/v2/Groups")["Resources"]

    """
    METHOD NAME: getGroupDetail  
    DESCRIPTION: 특정 그룹의 상세 정보 조회  
    PARAMETER:  
    - group_id: 그룹 ID  
    RETURN VALUE: 그룹 정보 (JSON)
    """
    def getGroupDetail(self, group_id):
        return self._get(f"/api/2.1/accounts/{self.account_id}/scim/v2/Groups/{group_id}")

    """
    METHOD NAME: createGroup  
    DESCRIPTION: 새로운 그룹 생성  
    PARAMETER:  
    - group_name: 생성할 그룹 이름  
    RETURN VALUE: 생성된 그룹 정보 (JSON)
    """
    def createGroup(self, group_name):
        data = {
            "schemas": ["urn:ietf:params:scim:schemas:core:2.0:Group"],
            "displayName": f"{group_name}"
        }
        return self._post(f"/api/2.1/accounts/{self.account_id}/scim/v2/Groups", data)

    """
    METHOD NAME: deleteGroup  
    DESCRIPTION: 특정 그룹 삭제  
    PARAMETER:  
    - group_id: 그룹 ID  
    RETURN VALUE: 성공 시 응답 JSON 또는 None
    """
    def deleteGroup(self, group_id):
        return self._delete(
            f"/api/2.0/accounts/{self.account_id}/scim/v2/Groups/{group_id}"
        )

    """
    METHOD NAME: removeUserFromGroup  
    DESCRIPTION: 특정 그룹에서 사용자를 제거  
    PARAMETER:  
    - group_id: 그룹 ID  
    - user_id: 사용자 ID  
    RETURN VALUE: 성공 시 응답 JSON 또는 None
    """
    def removeUserFromGroup(self, group_id, user_id):
        data = {
            "schemas": ["urn:ietf:params:scim:api:messages:2.0:PatchOp"],
            "Operations": [
                {
                    "op": "remove",
                    "path": f'members[value eq "{user_id}"]'
                }
            ]
        }
        return self._patch(f"/api/2.1/accounts/{self.account_id}/scim/v2/Groups/{group_id}", data)

    """
    METHOD NAME: resultDataframe  
    DESCRIPTION: 사용자와 소속 그룹 정보를 조인한 결과 반환  
    RETURN VALUE: Spark DataFrame(display, value, groupNames)
    """
    def resultDataframe(self):
        result = []

        for element in self.getGroupList():
            data = self.getGroupDetail(element["id"])

            if data.get('members'):
                for member in data["members"]:
                    member["groupName"] = data["displayName"]
                    result.append(member)

        df1 = spark.createDataFrame(result).select("display", "groupName", "value")
        df1 = df1.groupBy('display', 'value').agg(collect_list('groupName').alias('groupNames'))
        df2 = self.getUserList()

        df_result = df2.join(df1, df2.id == df1.value, "left_outer")

        return df_result


# main
if __name__ == "__main__":
    DATABRICKS_INSTANCE = "https://adb-1717497422236143.3.azuredatabricks.net"
    TOKEN = dbutils.secrets.get(scope="utils_admin_scope", key="admin_secret") #오너가 될 주체가 토큰을 발급받아야함

    api = TokenRefresher(databricks_instance=DATABRICKS_INSTANCE, token=TOKEN)

    token_info = api.listTokens()
    token_comments = [token['comment'] for token in token_info if isinstance(token, dict)] if token_info else []

    if 'admin_secret' not in token_comments:
        api.addSecret("utils_admin_scope", "admin_secret", api.createToken("admin_secret"))