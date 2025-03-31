# 노트북 오너 변경
from .base import DatabricksAPIBase

class NotebookOwnerManager(DatabricksAPIBase):
    def create_directory(self, path):
        return True if self._post("/api/2.0/workspace/mkdirs", {"path": path}) else None

    def export_notebook(self, source_path, export_format="DBC"):
        result = self._get("/api/2.0/workspace/export", params={"path": source_path, "format": export_format})
        return result.get("content") if result else None

    def import_notebook(self, target_path, base64_content, export_format="DBC"):
        if base64_content is None:
            return None

        data = {
            "path": target_path,
            "format": export_format,
            "language": "PYTHON",
            "overwrite": "false",
            "content": base64_content
        }
        return True if self._post("/api/2.0/workspace/import", data) else None

    def delete_notebook(self, target_path, recursive=False):
        result = self._post("/api/2.0/workspace/delete", {"path": target_path, "recursive": recursive})
        if result:
            print(f"노트북 {target_path} 삭제 완료")
            return True
        return None

    def get_list(self, notebook_path):
        result = self._get("/api/2.0/workspace/list", params={"path": notebook_path})
        return result.get("objects", []) if result else None

    def create_backup(self, source_path):
        prefix = "backup_"
        split_path = source_path.split('/')
        backup_file = prefix + split_path[-1]
        backup_path = source_path.replace(split_path[-1], backup_file)

        if os.path.exists(backup_path):
            print(f"백업 파일 {backup_path} 이미 존재")
            return None

        try:
            os.rename(source_path, backup_path)
            return backup_path
        except OSError as e:
            print(f"백업 생성 중 오류 발생: {e}")
            return None

    def owner_change(self, source_path, export_format="DBC", backup=True):
        prefix_check = source_path.split('/')[-1]
        if prefix_check.startswith("backup_") and backup:
            print("이미 백업된 파일이 존재합니다. 소유자 변경 중단.")
            return None

        base64_content = self.export_notebook(source_path, export_format)
        if base64_content is None:
            print("노트북 Export 실패. 소유자 변경 중단.")
            return None

        backup_path = self.create_backup(source_path)
        if backup_path is None:
            print("백업 파일 이름 변경 실패")
            return None
        print(f"기존 노트북을 백업 파일로 변경: {backup_path}")

        if not backup:
            if self.delete_notebook(backup_path) is None:
                print(f"백업된 파일 {backup_path} 삭제 실패")
                return None
            print(f"백업된 파일 {backup_path} 삭제 완료")

        import_result = self.import_notebook(source_path, base64_content, export_format)
        if import_result:
            print(f"새 소유자로 {source_path} 에 노트북 복사 완료")
            return True
        else:
            print(f"{source_path} 에 노트북 복사 실패. 원래 파일명으로 복구 중...")
            try:
                os.rename(backup_path, source_path)
                print(f"복구 완료: {source_path}")
            except OSError as e:
                print(f"원본 파일 복구 실패: {e}")
            return None
