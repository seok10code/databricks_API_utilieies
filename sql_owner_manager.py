# 쿼리, 알림, 대쉬보드 오너 변경
from .base import DatabricksAPIBase

class SQLOwnerManager(DatabricksAPIBase):
    def get_list(self, object_type, page_size=20):
        if object_type not in ["alerts", "queries"]:
            endpoint = f"/api/2.0/preview/sql/{object_type}"
        else:
            endpoint = f"/api/2.0/sql/{object_type}"

        object_list = []
        page_token = None

        while True:
            params = {"page_size": page_size}
            if page_token:
                params["page_token"] = page_token

            response = self._get(endpoint, params=params)
            if response:
                object_list.extend(response.get("results", []))
                page_token = response.get("next_page_token")
                if not page_token:
                    break
            else:
                print(f"API 요청 실패")
                break

        result = self.get_dataframe(object_type, object_list)
        return result if result else None

    def get_dataframe(self, object_type, object_list):
        if not object_list:
            return None

        if object_type == "queries":
            schema = StructType([
                StructField("id", StringType(), True),
                StructField("display_name", StringType(), True),
                StructField("owner_user_name", StringType(), True),
                StructField("warehouse_id", StringType(), True),
                StructField("query_text", StringType(), True),
                StructField("run_as_mode", StringType(), True),
                StructField("lifecycle_state", StringType(), True),
                StructField("last_modifier_user_name", StringType(), True),
                StructField("create_time", StringType(), True),
                StructField("update_time", StringType(), True),
                StructField("apply_auto_limit", BooleanType(), True),
                StructField("catalog", StringType(), True),
                StructField("schema", StringType(), True)
            ])
            return spark.createDataFrame(object_list, schema=schema)

        elif object_type == "alerts":
            schema = StructType([
                StructField('id', StringType(), True),
                StructField('display_name', StringType(), True),
                StructField('query_id', StringType(), True),
                StructField('state', StringType(), True),
                StructField('seconds_to_retrigger', IntegerType(), True),
                StructField('lifecycle_state', StringType(), True),
                StructField('trigger_time', StringType(), True),
                StructField('condition', StructType([
                    StructField('op', StringType(), True),
                    StructField('operand', StructType([
                        StructField('column', StructType([
                            StructField('name', StringType(), True)
                        ]), True)
                    ]), True),
                    StructField('threshold', StructType([
                        StructField('value', StructType([
                            StructField('double_value', DoubleType(), True),
                            StructField('string_value', StringType(), True)
                        ]), True)
                    ]), True),
                    StructField('empty_result_state', StringType(), True)
                ]), True),
                StructField('owner_user_name', StringType(), True),
                StructField('create_time', StringType(), True),
                StructField('update_time', StringType(), True),
                StructField('notify_on_ok', BooleanType(), True),
                StructField('custom_body', StringType(), True),
                StructField('custom_subject', StringType(), True)
            ])
            return spark.createDataFrame(object_list, schema=schema)

        elif object_type == "dashboards":
            schema = StructType([
                StructField("id", StringType(), True),
                StructField("slug", StringType(), True),
                StructField("name", StringType(), True),
                StructField("user_id", StringType(), True),
                StructField("dashboard_filters_enabled", BooleanType(), True),
                StructField("widgets", NullType(), True),
                StructField("options", StructType([
                    StructField("parent", StringType(), True),
                    StructField("run_as_role", StringType(), True),
                    StructField("folder_node_status", StringType(), True),
                    StructField("folder_node_internal_name", StringType(), True)
                ]), True),
                StructField("is_draft", BooleanType(), True),
                StructField("tags", ArrayType(StringType()), True),
                StructField("updated_at", StringType(), True),
                StructField("created_at", StringType(), True),
                StructField("version", IntegerType(), True),
                StructField("color_palette", ArrayType(StringType()), True),
                StructField("run_as_role", StringType(), True),
                StructField("run_as_service_principal_id", StringType(), True),
                StructField("data_source_id", StringType(), True),
                StructField("warehouse_id", StringType(), True),
                StructField("user", StructType([
                    StructField("id", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("email", StringType(), True)
                ]), True),
                StructField("is_favorite", BooleanType(), True)
            ])
            return spark.createDataFrame(object_list, schema=schema)

        else:
            print("지원하지 않는 object_type입니다.")
            return None

    def update_owner(self, df, object_type, owner_to, owner_from="all"):
        df_lst = self.get_list(object_type)
        if df_lst is None:
            print("오브젝트 리스트 불러오기 실패")
            return None

        type_map = {"queries": "query", "alerts": "alert", "dashboards": "dashboard"}
        object_type_key = type_map.get(object_type, object_type)

        if owner_from != "all":
            if object_type_key in ["query", "alert"]:
                df_lst = df_lst.filter(col("owner_user_name").contains(owner_from))
            else:
                df_lst = df_lst.withColumn("user_email", col("user.email"))
                df_lst = df_lst.filter(col("user_email").contains(owner_from))

        display(df_lst)

        for row in df_lst.collect():
            object_id = row["id"]
            endpoint = f"/api/2.0/preview/sql/permissions/{object_type_key}/{object_id}/transfer"
            payload = {"new_owner": owner_to}
            result = self._post(endpoint, data=payload)
            if result:
                print(f"[성공] {object_type_key} {object_id} → {owner_to}")
            else:
                print(f"[실패] {object_type_key} {object_id}")
        return True
