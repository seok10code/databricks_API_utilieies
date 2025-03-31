# ADLS로 노트북 업로드(백업)


class NotebookExporterToADLS:
    def __init__(self, databricks_instance, token, storage_account_name, container_name, sas_token):
        self.databricks_instance = databricks_instance.rstrip('/')
        self.token = token
        self.storage_account_name = storage_account_name
        self.container_name = container_name
        self.sas_token = sas_token

    def _get_headers(self):
        return {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

    def export_notebook(self, source_path, export_format="DBC"):
        url = f"{self.databricks_instance}/api/2.0/workspace/export"
        params = {"path": source_path, "format": export_format}
        response = requests.get(url, headers=self._get_headers(), params=params)

        if response.status_code != 200:
            print(f"노트북 다운로드 실패, 코드: {response.status_code}, 응답: {response.text}")
            return None

        content = response.json().get("content")
        if not content:
            print("응답에 'content' 필드가 없습니다.")
            return None

        return base64.b64decode(content)

    def upload_to_adls(self, directory_name, file_name, decoded_content):
        try:
            service_client = DataLakeServiceClient(
                account_url=f"https://{self.storage_account_name}.dfs.core.windows.net",
                credential=self.sas_token
            )
            file_system_client = service_client.get_file_system_client(self.container_name)
            directory_client = file_system_client.get_directory_client(directory_name)
            file_client = directory_client.create_file(file_name)

            file_client.append_data(decoded_content, offset=0, length=len(decoded_content))
            file_client.flush_data(len(decoded_content))

            print(f"ADLS 저장 완료: abfss://{self.container_name}@{self.storage_account_name}.dfs.core.windows.net/{directory_name}/{file_name}")
            return True
        except Exception as e:
            print(f"ADLS 업로드 실패: {e}")
            return None

    def export_and_upload(self, source_path, directory_name, file_name, export_format="DBC"):
        decoded_content = self.export_notebook(source_path, export_format)
        if decoded_content is None:
            return None
        return self.upload_to_adls(directory_name, file_name, decoded_content)
