import json
import traceback
import os
import boto3
from boto3.dynamodb.conditions import Key
from decimal import Decimal


class WebSocket:
    # connection apigateway
    def connect_apigw():
        try:
            apigw_management = boto3.client(
                "apigatewaymanagementapi",
                endpoint_url="",
            )
            return apigw_management
        except:
            print("connection apigateway error")
            traceback.print_exc()

    def get_websocket_table():
        try:
            dynamo_db = boto3.resource(
                service_name="dynamodb", region_name="ap-northeast-1"
            )
            table = dynamo_db.Table("WebSocket")
            return table
        except:
            print("get item of websocket error!!!")
            traceback.print_exc()

    # get connection_id from websocketTable
    def get_connection_id(table):
        try:
            scanData = table.scan()
            items = scanData["Items"]
            return items
        except:
            print("get connection id error")
            traceback.print_exc()

    # websocketの接続元にレスポンスを返す
    def post_to_connection(apigw_management, connection_id, res_data):
        try:
            convert_res_data = json.dumps(res_data)
            print("res", convert_res_data)
            apigw_management.post_to_connection(
                ConnectionId=connection_id, Data=convert_res_data
            )
        except:
            print("post_to_connection error")
            traceback.print_exc()
