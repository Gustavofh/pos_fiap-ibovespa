import json

data = {
  "Records": [
    {
      "messageId": "961f049c-34cb-44ba-afa1-5d443a86df9a",
      "receiptHandle": "AQEB6T5L9y8PXgYAuphixx+CB74UhpWp2uK9WQmgJQug3sy0d5IJi/CkruaD7gxdDrGLGw+9QmVitEMgEPxoarCe+F+5UnmM/68iA9tugzkD2DRfOMSZmHiKVf3w5zN2j2EfBq/EPsZUkPsq6dk6f/nyPii2gqhSXp5Vsc2+QtsfhVSZpYOwaZiEU7XdltZk2aDWCBlO+em/M0KRmRhTDy9wG8hgXrI0APJXqzggccjoTz4/Fi8NohL2NDlCTtiw07G/MVuaKvt79768ns+IdnnLSgoHtYXRBpbL57qiRptbru17f/ITY11FgmhM/qi+05e/tSMHVuhwvLEjq7K5MkJmnCIRWrcMm/ZJTQT6ujLch+MNRVtRRJrGfGhkbZRtadoLsSVGkMhZD71oWc6+NsT1LA==",
      "body": {
        "Records": [
          {
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "awsRegion": "us-east-1",
            "eventTime": "2025-07-11T02:31:21.926Z",
            "eventName": "ObjectCreated:Put",
            "userIdentity": {
              "principalId": "AWS:AIDAVZI45Z5VGDBGV5ZWO"
            },
            "requestParameters": {
              "sourceIPAddress": "189.6.214.107"
            },
            "responseElements": {
              "x-amz-request-id": "4YSJN1BEK18QAA9D",
              "x-amz-id-2": "+j221+SmRnEG0vRhqDsnEL/HOccQa9c5fAwyyRslytF2jJ+unWnQhiTjjsxfbpj+VVd5vm+jX6QUnnHZNjtvTBY7IhVBmC5l"
            },
            "s3": {
              "s3SchemaVersion": "1.0",
              "configurationId": "test-lambda-update",
              "bucket": {
                "name": "pos-fiap-ibovespa-data",
                "ownerIdentity": {
                  "principalId": "AOL1V5WTOPJ18"
                },
                "arn": "arn:aws:s3:::pos-fiap-ibovespa-data"
              },
              "object": {
                "key": "raw/setor/pregao_diario.parquet",
                "size": 9403,
                "eTag": "b527aba882298b53f827b6f02d90572a",
                "sequencer": "0068707779B38F8FCB"
              }
            }
          }
        ]
      },
      "attributes": {
        "ApproximateReceiveCount": "1",
        "SentTimestamp": "1752201082297",
        "SenderId": "AROA4R74ZO52XAB5OD7T4:S3-PROD-END",
        "ApproximateFirstReceiveTimestamp": "1752201082298"
      },
      "messageAttributes": {},
      "md5OfBody": "f16e96a3fb00c7f118fce8a8e1ba6eed",
      "eventSource": "aws:sqs",
      "eventSourceARN": "arn:aws:sqs:us-east-1:397882019690:test-ibovespa",
      "awsRegion": "us-east-1"
    }
  ]
}


def print_json(data):
    for record in data['Records']:
        body = record['body']
        if isinstance(body, str):
            body = json.loads(body)

        for s3rec in body['Records']:
            bucket = s3rec['s3']['bucket']['name']
            key    = s3rec['s3']['object']['key']
            print(f"Bucket: {bucket} | Key: {key}")
print_json(data)