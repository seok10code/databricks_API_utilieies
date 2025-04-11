class NotebookOwnerManager(NotebookOpsBase):
    """
    METHOD NAME: createBackup  
    DESCRIPTION: 노트북 파일을 백업 파일명으로 이름 변경하여 백업 생성  
    PARAMETER:  
    - source_path: 원본 노트북 경로  
    RETURN VALUE: 백업된 파일 경로 또는 실패 시 None
    """
    def createBackup(self, source_path, prefix="backup"):
        base_dir = "/".join(source_path.split("/")[:-1])
        original_file = source_path.split("/")[-1]

        # 첫 시도: backup_original
        backup_file = f"{prefix}_{original_file}"
        backup_path = f"{base_dir}/{backup_file}"

        i = 2
        while os.path.exists(backup_path):
            backup_file = f"{prefix}{i}_{original_file}"
            backup_path = f"{base_dir}/{backup_file}"
            i += 1

        try:
            os.rename(source_path, backup_path)
            return backup_path
        except OSError as e:
            print(f"백업 생성 중 오류 발생: {e}")
            return None

    """
    METHOD NAME: ownerChange  
    DESCRIPTION: 기존 노트북을 백업 후 다시 import하여 소유자를 변경  
    PARAMETER:  
    - source_path: 소유자 변경 대상 노트북 경로  
    - export_format: 노트북 포맷 (기본값: "DBC")  
    - backup: 백업 여부 (기본값: True)  
    RETURN VALUE: 성공 시 True, 실패 시 None
    """
    def ownerChange(self, source_path, export_format="DBC", backup=True):
        base64_content = self.exportNotebook(source_path, export_format)
        if base64_content is None:
            print("노트북 Export 실패. 소유자 변경 중단.")
            return None

        backup_path = self.createBackup(source_path)
        if backup_path is None:
            print("백업 파일 이름 변경 실패. 소유자 변경 중단.")
            return None
        print(f"기존 노트북을 백업 파일로 변경: {backup_path}")

        import_result = self.importNotebook(source_path, base64_content, export_format)
        if import_result:
            print(f"새 소유자로 {source_path} 에 노트북 복사 완료")

            if not backup:
                if self.deleteNotebook(backup_path) is None:
                    print(f"백업된 파일 {backup_path} 삭제 실패")
                    return None
                print(f"백업된 파일 {backup_path} 삭제 완료")

            return True

        else:
            print(f"{source_path} 에 노트북 복사 실패. 원래 파일명으로 복구 중...")

            if os.path.exists(backup_path):
                try:
                    os.rename(backup_path, source_path)
                    print(f"복구 완료: {source_path}")
                except OSError as e:
                    print(f"원본 파일 복구 실패: {e}")
            else:
                print("복구 실패: 백업 파일이 존재하지 않음.")
            return None

# 쿼리, 알림, 대쉬보드 오너 변경
"""
CLASS NAME: SQLOwnerManager  
DESCRIPTION: Databricks SQL의 쿼리, 알림, 대시보드에 대한 오너(owner) 변경 및 목록 조회 기능을 담당하는 클래스  
INHERITS: DatabricksAPIBase
"""