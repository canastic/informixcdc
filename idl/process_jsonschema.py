
def set_properties_as_required_by_default(schema):
    def set_in_list_entries(o, *args):
        for k in args:
            for sub in o.get(k, []):
                set_properties_as_required_by_default(sub)

    def set_in_object_entries(o, *args):
        for k in args:
            try:
                set_properties_as_required_by_default(o[k])
            except KeyError:
                pass

    set_in_list_entries(schema, "allOf", "anyOf", "oneOf")
    set_in_object_entries(schema, "not", "if", "then", "else")
    for _, sub in schema.get("definitions", {}).items():
        set_properties_as_required_by_default(sub)

    type_ = schema.get("type", None)
    if type_ == "array":
        items = schema.get("items", [])
        if isinstance(items, dict):
            set_properties_as_required_by_default(items)
        else:
            for p in items:
                set_properties_as_required_by_default(p)

        set_in_object_entries(schema, "additionalItems", "contains")

    elif type_ == "object":
        required = set(schema.get("required", set()))
        for name, p in schema.get("properties", {}).items():
            if p.pop("required", True):
                required.add(name)
            set_properties_as_required_by_default(p)
        schema["required"] = list(required)

        set_in_list_entries(schema, "patternProperties")
        set_in_object_entries(schema, "additionalProperties", "additionalItems")

        for _, dep in schema.get("dependencies", []):
            if isinstance(dep, list):
                continue
            set_properties_as_required_by_default(dep)

def to_json(schema):
    set_properties_as_required_by_default(schema)
    schema["$schema"] = "http://json-schema.org/draft-07/schema#"

    import json
    return json.dumps(schema)
