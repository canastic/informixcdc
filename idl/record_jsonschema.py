#!/usr/bin/env python3

schema = {"allOf": [
    {"type": "object", "properties": [
        ("seq", {"type": "integer"}),
        ("transaction_id", {"type": "integer"}),
    ]},
    {"oneOf": [
        {"type": "object", "properties": [
            ("type", {"const": "begin_tx"}),
            ("start_at", {"type": "string", "format": "date-time"}),
            ("user_id", {"type": "integer"}),
        ]},
        {"type": "object", "properties": [
            ("type", {"const": "commit_tx"}),
            ("start_at", {"type": "string", "format": "date-time"}),
        ]},
        {"type": "object", "properties": [
            ("type", {"const": "rollback_tx"}),
        ]},
        {"type": "object", "$comment": "klaxon{common_class=row_image}", "properties": [
            ("type", {"enum": [
                "insert",
                "delete",
                "before_update",
                "after_update",
            ]}),
            ("table", {"type": "string"}),
            ("database", {"type": "string"}),
            ("owner", {"oneOf": [
                {"type": "string"},
                {"type": "null"}
            ]}),
            ("values", {
                "type": "object",
                "$comment": "klaxon{value_name=ColumnValue}",
                "additionalProperties": {
                    "type": "object",
                    "properties": [
                        ("raw", {"type": "string", "$comment": "base64"}),
                        ("decoded", {}),
                    ],
                },
            }),
        ]},
        {"type": "object", "properties": [
            ("type", {"const": "discard"}),
        ]},
        {"type": "object", "properties": [
            ("type", {"const": "truncate"}),
            ("table", {"type": "string"}),
            ("database", {"type": "string"}),
            ("owner", {"oneOf": [
                {"type": "string"},
                {"type": "null"}
            ]}),
        ]},
    ]},
]}

from process_jsonschema import to_json
print(to_json(schema))
