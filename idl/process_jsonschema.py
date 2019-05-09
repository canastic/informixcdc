from collections import OrderedDict

def process(schema):
    def set_in_list_entries(o, *args):
        for k in args:
            for sub in o.get(k, []):
                process(sub)

    def set_in_object_entries(o, *args):
        for k in args:
            try:
                process(o[k])
            except KeyError:
                pass

    set_in_list_entries(schema, "allOf", "anyOf", "oneOf")
    set_in_object_entries(schema, "not", "if", "then", "else")
    for _, sub in schema.get("definitions", {}).items():
        process(sub)

    type_ = schema.get("type", None)
    if type_ == "array":
        items = schema.get("items", [])
        if isinstance(items, dict):
            process(items)
        else:
            for p in items:
                process(p)

        set_in_object_entries(schema, "additionalItems", "contains")

    elif type_ == "object":
        required = set(schema.get("required", set()))
        for name, p in schema.get("properties", []):
            if p.pop("required", True):
                required.add(name)
            process(p)
        schema["required"] = list(required)

        if "properties" in schema:
            schema["$propertiesOrder"] = [k for k, _ in schema["properties"]]
            schema["properties"] = OrderedDict(schema["properties"])

        set_in_list_entries(schema, "patternProperties")
        set_in_object_entries(schema, "additionalProperties", "additionalItems")

        for _, dep in schema.get("dependencies", []):
            if isinstance(dep, list):
                continue
            process(dep)

def to_json(schema):
    process(schema)
    schema["$schema"] = "http://json-schema.org/draft-07/schema#"

    import json
    return json.dumps(schema)
