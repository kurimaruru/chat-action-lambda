import boto3
from boto3.dynamodb.conditions import Key
import traceback


class Dynamodb:
    def connect_db():
        try:
            dynamo_db = boto3.resource(
                service_name="dynamodb",
                region_name="ap-northeast-1",
            )
            return dynamo_db
        except:
            print("connect db error")
            traceback.print_exc()

    # 全権取得
    def scan(table):
        try:
            scanData = table.scan()
            items = scanData["Items"]
            return items
        except:
            print("scan error")
            traceback.print_exc()

    # レコード追加・更新
    def put(table, datas):
        try:
            # putRes = table.put_item(Item={"id": partitionKey})
            putRes = table.put_item(
                Item={
                    "userId": datas["userId"],
                    "sortKey": datas["sortKey"],
                    "message": datas["message"],
                }
            )
            if putRes["ResponseMetadata"]["HTTPStatusCode"] != 200:
                print(putRes)
            else:
                print("PUT Successed.")
            return putRes
        except:
            print("put error")
            traceback.print_exc()
