class NotebookOpsBase(DatabricksAPIBase):
    """
    METHOD NAME: createDirectory  
    DESCRIPTION: 지정된 경로에 디렉토리를 생성  
    PARAMETER:  
    - path: 생성할 디렉토리 경로  
    RETURN VALUE: 성공 시 True, 실패 시 None
    """
    def createDirectory(self, path):
        return True if self._post("/api/2.0/workspace/mkdirs", {"path": path}) else None

    """
    METHOD NAME: exportNotebook  
    DESCRIPTION: 지정된 경로의 노트북을 내보내고 Base64 인코딩된 내용을 반환  
    PARAMETER:  
    - source_path: 내보낼 노트북 경로  
    - export_format: 내보낼 포맷 (기본값: "DBC")  
    RETURN VALUE: Base64 인코딩된 노트북 내용 또는 None
    """
    def exportNotebook(self, source_path, export_format="DBC"):
        result = self._get("/api/2.0/workspace/export", params={"path": source_path, "format": export_format})
        return result.get("content") if result else None

    """
    METHOD NAME: importNotebook  
    DESCRIPTION: Base64 인코딩된 노트북을 지정된 경로에 가져오기  
    PARAMETER:  
    - target_path: 노트북이 저장될 경로  
    - base64_content: Base64 인코딩된 노트북 내용  
    - export_format: 노트북 포맷 (기본값: "DBC")  
    RETURN VALUE: 성공 시 True, 실패 시 None
    """
    def importNotebook(self, target_path, base64_content, export_format="DBC"):
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

    """
    METHOD NAME: deleteNotebook  
    DESCRIPTION: 지정된 경로의 노트북을 삭제  
    PARAMETER:  
    - target_path: 삭제할 노트북 경로  
    - recursive: 디렉토리일 경우 하위 항목 포함 여부 (기본값: False)  
    RETURN VALUE: 성공 시 True, 실패 시 None
    """
    def deleteNotebook(self, target_path, recursive=False):
        result = self._post("/api/2.0/workspace/delete", {"path": target_path, "recursive": recursive})
        if result:
            print(f"노트북 {target_path} 삭제 완료")
            return True
        return None

    """
    METHOD NAME: getList  
    DESCRIPTION: 주어진 경로 아래의 노트북/폴더 목록을 반환  
    PARAMETER:  
    - notebook_path: 조회할 경로  
    RETURN VALUE: 객체 리스트 (list of notebooks/folders) 또는 빈 리스트
    """
    def getList(self, notebook_path):
        result = self._get("/api/2.0/workspace/list", params={"path": notebook_path})
        return result.get("objects", []) if result else None
    

    def getNotebookList(self, notebook_path):
        file_list = []
        for root, dirs, files in os.walk(notebook_path):
            files = api_notebook.getList(root)
            if files:
                file_list += files
        result = spark.createDataFrame(file_list)
        display(result)
        return result


# 노트북 오너 변경
"""
CLASS NAME: NotebookOwnerManager  
DESCRIPTION: Databricks 노트북의 백업, 복사, 삭제, 오너 변경 등의 기능을 수행하는 클래스  
INHERITS: NotebookOpsBase
"""