import json
import traceback
from dynamodb import Dynamodb

# from dynamodb import connect_db,put,scan
from websocket import WebSocket


def lambda_handler(event, context):
    try:
        print("event = ", json.dumps(event))
        # connection table
        dynamodb = Dynamodb.connect_db()
        table = dynamodb.Table("Chat")
        # websocket接続情報取得
        websocket_table = WebSocket.get_websocket_table()
        print(websocket_table)
        connect_ids = WebSocket.get_connection_id(websocket_table)
        print("get connectionId = ", connect_ids)
        # クライアントからのリクエスト情報
        dict_body = json.loads(event["body"])
        operationType = dict_body["data"]["OperationType"]
        # connection api gateway
        apigw_management = WebSocket.connect_apigw()
        if operationType == "openChat":
            res = Dynamodb.scan(table)
            print("scan table res = ", res)
            res_data = {"key": "chatOpenRes", "data": res}
            try:
                for id in connect_ids:
                    WebSocket.post_to_connection(
                        apigw_management, id["connection_id"], res_data
                    )
            except:
                print("post_to_connection error ", id)
        elif operationType == "sendMessage":
            Dynamodb.put(table, dict_body["data"])
            res_data = {"key": "sendRes", "data": [dict_body["data"]]}
            try:
                for id in connect_ids:
                    WebSocket.post_to_connection(
                        apigw_management, id["connection_id"], res_data
                    )
            except:
                print("post_to_connection error ", id)
        else:
            print("not operationType")

        return {
            "statusCode": 200,
            "headers": {"Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"result": 0}, ensure_ascii=False),
        }
    except:
        print("error!!!")
        traceback.print_exc()
