#!/usr/bin/env python3

schema = {
    "type": "object",
    "properties": [
        ("from_seq", {"oneOf": [
            {"type": "integer"},
            {"type": "null"}
        ]}),
        ("tables", {
            "type": "array",
            "items": {"type": "object", "properties": [
                ("name", {"type": "string"}),
                ("database", {"type": "string"}),
                ("owner", {"type": "string"}),
                ("columns", {"oneOf": [
                    {"type": "null"},
                    {"type": "array", "items": {"type": "string"}}
                ]})
            ]}
        })
    ]
}

from process_jsonschema import to_json

print(to_json(schema))
