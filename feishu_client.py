import json
import time
import lark_oapi as lark
from lark_oapi.api.bitable.v1 import *
from typing import List, Optional
from pydantic import BaseModel, validator


class FieldsModel(BaseModel):
    app_id: str
    name: str
    display: str
    account: Optional[str] = None
    update_time: int = int(time.time() * 1000)
    remark: Optional[str] = None

    # 定义枚举值的映射
    _display_enum = {
        "configuring": "正在创建",
        "configured": "创建完成",
        "effective": "生效",
        "invalidated": "失效",
        "rejected": "拒绝",
    }

    # 验证display字段
    @validator('display')
    def check_display(cls, v):
        for key, value in cls._display_enum.items():
            if v in (key, value):
                return key
        raise ValueError(f"Display must be one of {list(cls._display_enum.values())}")

    # 覆盖父类的dict方法
    def dict(self, **kwargs):
        d = super().dict(**kwargs)
        d['display'] = self._display_enum[self.display]
        return d


class RecordModel(BaseModel):
    fields: FieldsModel
    id: Optional[str] = None
    record_id: Optional[str] = None


class FeishuClient:
    def __init__(self, app_id: str, app_secret: str, app_token: str, table_id: str):
        """
        :param app_id: 企业自建应用的app_id （获取地址：https://open.feishu.cn/app/）
        :param app_secret: 企业自建应用的app_secret
        :param app_token: 从需要控制的多维表格url中找
        :param table_id: 从需要控制的多维表格url中找
        """
        self.client = lark.Client.builder().app_id(app_id) \
            .app_secret(app_secret) \
            .enable_set_token(True) \
            .log_level(lark.LogLevel.DEBUG) \
            .build()
        self.app_token = app_token
        self.table_id = table_id

    def query_record(self, key: str, value: str) -> List[RecordModel]:
        page_token = None
        records = []
        while True:
            request = ListAppTableRecordRequest.builder() \
                .app_token(self.app_token) \
                .table_id(self.table_id) \
                .filter(f'CurrentValue.[{key}] = "{FieldsModel._display_enum[value] if key == "display" else value}"') \
                .page_size(500)
            if page_token:
                request.page_token(page_token)
            request = request.build()
            response: ListAppTableRecordResponse = self.client.bitable.v1.app_table_record.list(request)

            if not response.success():
                lark.logger.error(
                    f"client.bitable.v1.app_table_record.list failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}")
                raise Exception(f"获取飞书文档{key}={value}失败")

            data = lark.JSON.marshal(response.data, indent=4)
            lark.logger.info(data)
            json_data = json.loads(data)
            page_token = json_data.get("page_token", None)
            if json_data["total"] > 0:
                records += [RecordModel(**item) for item in json_data.get("items", [])]
            if not page_token:
                break
        return records

    def add_record(self, records: List[FieldsModel]):
        new_records = [AppTableRecord.builder().fields(b.dict()).build() for b in records]
        request = BatchCreateAppTableRecordRequest.builder() \
            .app_token(self.app_token) \
            .table_id(self.table_id) \
            .request_body(BatchCreateAppTableRecordRequestBody.builder().records(new_records).build()) \
            .build()

        response: BatchCreateAppTableRecordResponse = self.client.bitable.v1.app_table_record.batch_create(request)
        if not response.success():
            lark.logger.error(
                f"client.bitable.v1.app_table_record.batch_create failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}")
            raise Exception(f"增加飞书内容失败：{request}")

        data = lark.JSON.marshal(response.data, indent=4)
        lark.logger.info(data)
        return data

    def update_record(self, records: List[RecordModel]):
        change_records = [AppTableRecord.builder().fields(b.fields.dict()).record_id(b.record_id).build() for b in
                          records]
        request = BatchUpdateAppTableRecordRequest.builder() \
            .app_token(self.app_token) \
            .table_id(self.table_id) \
            .request_body(BatchUpdateAppTableRecordRequestBody.builder().records(change_records).build()) \
            .build()

        response: BatchUpdateAppTableRecordResponse = self.client.bitable.v1.app_table_record.batch_update(request)
        if not response.success():
            lark.logger.error(
                f"client.bitable.v1.app_table_record.batch_update failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}")
            raise Exception(f"修改飞书内容失败：{request}")

        data = lark.JSON.marshal(response.data, indent=4)
        lark.logger.info(data)
        return data

    def delete_record(self, records: List[RecordModel]):
        record_ids = [b.record_id for b in records]
        # 构造请求对象
        request = BatchDeleteAppTableRecordRequest.builder() \
            .app_token(self.app_token) \
            .table_id(self.table_id) \
            .request_body(BatchDeleteAppTableRecordRequestBody.builder().records(record_ids).build()) \
            .build()

        response: BatchDeleteAppTableRecordResponse = self.client.bitable.v1.app_table_record.batch_delete(request)
        if not response.success():
            lark.logger.error(
                f"client.bitable.v1.app_table_record.batch_delete failed, code: {response.code}, msg: {response.msg}, log_id: {response.get_log_id()}")
            raise Exception(f"删除飞书内容失败：{request}")

        data = lark.JSON.marshal(response.data, indent=4)
        lark.logger.info(data)
        return data
