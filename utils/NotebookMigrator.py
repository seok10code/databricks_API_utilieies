# 디렉토리 아래 존재하는 노트북 전체 복사 (폴더 트리 반영)
"""
CLASS NAME: NotebookMigrator  
DESCRIPTION: Databricks Workspace의 디렉토리 구조와 노트북을 전체 복사(migration)하는 기능을 수행

INHERITS: NotebookOpsBase
"""

class NotebookMigrator(NotebookOpsBase):
    """
    METHOD NAME: getDirectoryList
    DESCRIPTION: 지정된 basePath 내 디렉토리 및 노트북 리스트를 재귀적으로 수집  
    PARAMETER:  
    - basePath: 시작 경로  
    RETURN VALUE: 전체 객체 리스트
    """
    def getDirectoryList(self, basePath):
        result = []
        for root, dirs, file in os.walk(basePath):
            for dir_name in dirs:
                sourcePath = os.path.join(root, dir_name)
                if self.getList(sourcePath):
                    result += self.getList(sourcePath)
        result += self.getList(basePath)
        return result

    """
    METHOD NAME: migrate  
    DESCRIPTION: 하나의 노트북을 source에서 target으로 복사  
    PARAMETER:  
    - source_path: 복사할 노트북 경로  
    - target_path: 복사 대상 경로  
    - export_format: 포맷 (기본값: "DBC")  
    RETURN VALUE: 성공 시 True, 실패 시 None
    """
    def migrate(self, source_path, target_path, export_format="DBC"):
        base64_content = self.exportNotebook(source_path, export_format)
        if base64_content is None:
            print("노트북 Export 실패. 복사 중단.")
            return None

        import_result = self.importNotebook(target_path, base64_content, export_format)
        if import_result:
            print(f"{target_path} 에 노트북 복사 완료")
            return True
        else:
            print("노트북 Import 실패.")
            return None

    """
    METHOD NAME: migrationV1  
    DESCRIPTION: 폴더 구조는 유지하지 않고, 노트북만 복사 후 삭제  
    PARAMETER:  
    - source_path: 복사할 상위 경로  
    - target_path: 붙여넣을 대상 경로  
    """
    def migrationV1(self, source_path, target_path):
        df = spark.createDataFrame(self.getDirectoryList(source_path)).filter(col('object_type') == 'NOTEBOOK')\
        .select("path")

        for sourcePath in df.collect():
            file_name = sourcePath['path'].split("/")[-1]

            if target_path[-1] != "/":
                target_path += "/"
            target_path += file_name

            self.migrate(sourcePath['path'], target_path, EXPORT_FORMAT)
            
            # self.deleteNotebook(target_path)
            target_path = target_path[:-len(file_name)]
            return True

    """
    METHOD NAME: migrationV2  
    DESCRIPTION: 디렉토리 구조와 노트북을 유지하며 전체 복사  
    PARAMETER:  
    - source_path: 원본 경로  
    - target_path: 복사 대상 경로  
    RETURN VALUE: 항상 True 반환
    """
    def migrationV2(self, source_path, target_path):
        df = spark.createDataFrame(self.getDirectoryList(source_path))
        df_dir = df.filter(col('object_type') == 'DIRECTORY')
        df_note = df.filter(col('object_type') == 'NOTEBOOK')

        for row in df_dir.collect():
            if os.path.exists(row['path']):
                print(f"디렉토리 {row['path']}은 이미 존재합니다.")
            else:
                self.createDirectory(row['path'].replace(source_path, target_path))

        for row in df_note.collect():
            if os.path.exists(row['path'].replace(source_path, target_path)):
                print(f"노트북 {row['path'].replace(source_path, target_path)}은 이미 존재합니다.")
            else:
                self.migrate(row['path'], row['path'].replace(source_path, target_path), EXPORT_FORMAT)

        return True
