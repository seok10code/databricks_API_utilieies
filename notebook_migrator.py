# 디렉토리 아래 존재하는 노트북 전체 복사
from .base import DatabricksAPIBase

class NotebookMigrator(DatabricksAPIBase):
    def create_directory(self, path):
        result = self._post("/api/2.0/workspace/mkdirs", {"path": path})
        if result:
            print(f"디렉토리 생성: {path}")
            return True
        return None

    def export_notebook(self, source_path, export_format="DBC"):
        result = self._get("/api/2.0/workspace/export", params={"path": source_path, "format": export_format})
        return result.get("content") if result else None

    def import_notebook(self, target_path, base64_content, export_format="DBC"):
        if base64_content is None:
            return None

        if os.path.exists(target_path):
            print(f"경로 {target_path}은 이미 존재합니다. 소스 경로에 이름이 같은 파일이 있는지 확인하세요")
            return None

        data = {
            "path": target_path,
            "format": export_format,
            "language": "PYTHON",
            "overwrite": "false",
            "content": base64_content
        }
        result = self._post("/api/2.0/workspace/import", data)
        return True if result else None

    def delete_notebook(self, target_path, recursive=False):
        result = self._post("/api/2.0/workspace/delete", {"path": target_path, "recursive": recursive})
        if result:
            print(f"노트북 {target_path} 삭제 완료")
            return True
        return None

    def list_items(self, notebook_path):
        result = self._get("/api/2.0/workspace/list", params={"path": notebook_path})
        return result.get("objects", []) if result else None

    def migrate(self, source_path, target_path, export_format="DBC"):
        base64_content = self.export_notebook(source_path, export_format)
        if base64_content is None:
            print("노트북 Export 실패. 복사 중단.")
            return None

        import_result = self.import_notebook(target_path, base64_content, export_format)
        if import_result:
            print(f"{target_path} 에 노트북 복사 완료")
            return True
        else:
            print("노트북 Import 실패.")
            return None
