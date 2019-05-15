#!/usr/bin/env python3

from copy import copy
from collections import OrderedDict
import re

class Class:
    def __init__(self, name, properties, inner_types, parent=None, embed_ref=None, sealed=False):
        self.name = name
        self.properties = properties
        self.inner_types = inner_types
        self.parent = parent
        self.embed_ref = embed_ref
        self.sealed = sealed

class Union(Class):
    pass

class UnionByType(Union):
    def __init__(self, name, properties, inner_types, variants, parent=None, embed_ref=None):
        super().__init__(name, properties, inner_types, parent=parent, embed_ref=embed_ref, sealed=True)
        # [(<jsonschema type ("number", "string", etc...)>, <variant class name>)]
        self.variants = variants

class UnionByField(Union):
    def __init__(self, name, properties, inner_types, discriminant_json, variants, parent=None, embed_ref=None):
        super().__init__(name, properties, inner_types, parent=parent, embed_ref=embed_ref, sealed=True)
        self.discriminant_json = discriminant_json
        # [(<discriminant field value>, <variant class name>, <variant properties>)]
        self.variants = variants

class Enum():
    def __init__(self, name, variants):
        self.name = name
        self.variants = variants

class Property:
    def __init__(self, json_name, kt_name, type_, default):
        self.json_name = json_name
        self.kt_name = kt_name
        self.type = type_
        self.default = default

def declarations_from_schema(schema, name):
    _, decls = extract(schema, name)
    return decls

def extract(schema, name):
    if "oneOf" in schema:
        return extract_oneof(schema["oneOf"], name)

    if "$ref" in schema:
        return ref_name(schema), []

    if "type" in schema:
        type_ = schema["type"]
        class_name = basic_class(schema)
        if class_name is not None:
            return class_name, []
        if type_ == "object":
            if "properties" not in schema:
                name = extract_directive(schema, "value_name") or f"{name}Value"
                name, types = extract(schema.get("additionalProperties", {}), name)
                return f"Map<String, {name}>", types
            properties, inner_types, embed_ref = extract_object(schema)
            return name, [Class(name, properties, inner_types, embed_ref=embed_ref)]
        if type_ == "array":
            items_schema = schema.get("items", {})
            name = extract_directive(schema, "element_name") or f"{name}Element"
            name, types = extract(items_schema, name)
            return f"List<{name}>", types
        assert(False)

    if "enum" in schema and all(isinstance(x, str) for x in schema["enum"]):
        return name, [Enum(name, schema["enum"])]

    if (
        "allOf" in schema
        and len(schema["allOf"]) == 2
        and len([sub for sub in schema["allOf"] if sub.get("type") == "object"]) == 1
        and len([sub for sub in schema["allOf"] if "oneOf" in sub]) == 1
    ):
        subs = schema["allOf"]
        common_fields_schema = [sub for sub in subs if sub.get("type") == "object"][0]
        variants_schema = [sub for sub in subs if "oneOf" in sub][0]["oneOf"]

        name, union = extract_oneof(variants_schema, name)
        union = union[0]

        _, fields_class = extract(common_fields_schema, name)
        fields_class = fields_class[0]
        union.properties = fields_class.properties
        union.inner_types += fields_class.inner_types

        return name, [union]

    return "Any?", []

def basic_class(schema):
    type_ = schema.get("type")
    if type_ == "integer":
        return "Long"
    if type_ == "number":
        return "java.math.BigDecimal"
    if type_ == "string":
        format = schema.get("format", None)
        # if format == "date":
        #     return "JSONDate"
        return "String"
    if type_ == "boolean":
        return "Bool"
    return None

def ref_name(schema):
    return camel(schema["$ref"].split("/")[-1], capitalize=True)

def extract_oneof(schemas, name):
    assert(isinstance(schemas, list))

    # oneOfs define union types. We need some discriminant to decide which
    # variant a particular value of this union type should be mapped to. We
    # support these strategies to determine the discriminant:
    #
    # - Each schema has type "object", and among their required properties
    #   there's a const or enum with values that don't overlap between schemas.
    # - Each schema has a different "type" field.
    # 
    # In any case, a {"type": "null"} variant just wraps the union type in an
    # optional.

    add_optional = False
    if is_optional(schemas):
        schemas = remove_optional(schemas)
        add_optional = True
        if len(schemas) == 1:
            # Special case: it's a single optional type.
            type_, new_types = extract(schemas[0], name)
            return type_ + "?", new_types

    type_, new_types, ok = extract_oneof_discriminant_field(schemas, name)
    if not ok:
        type_, new_types, ok = extract_oneof_different_types(schemas, name)
    if not ok:
        assert(False)

    if add_optional:
        type_ += "?"
    return type_, new_types

def is_optional(schemas):
    return any(schema.get("type") == "null" for schema in schemas)

def remove_optional(schemas):
    return [schema for schema in schemas if schema.get("type") != "null"]

def extract_oneof_discriminant_field(schemas, name):
    # property name -> [(enum + const values, property schema)]
    possible_discriminants = {}

    for schema in schemas:
        if schema.get("type") != "object":
            return None, None, False

        ppties = schema.get("properties", None)
        if ppties is None:
            return None, None, False

        for ppty_name, ppty_schema in ppties.items():
            values = OrderedSet(ppty_schema.get("enum", []))
            if "const" in  ppty_schema:
                values = values.union(OrderedSet([ppty_schema["const"]]))
            if len(values) == 0:
                continue

            discriminant_variants = possible_discriminants.get(ppty_name, [])
            possible_discriminants[ppty_name] = discriminant_variants

            values_are_unique = True
            for tag_values, _ in discriminant_variants:
                if len(tag_values.intersection(values)) > 0:
                    values_are_unique = False
                    break

            if values_are_unique:
                discriminant_variants.append((values, schema))
            else:
                del possible_discriminants[ppty_name]

    for ppty, variants in list(possible_discriminants.items()):
        if len(variants) != len(schemas):
            del possible_discriminants[ppty]

    if len(possible_discriminants) != 1:
        return None, None, False

    discriminant_json_name, discriminant_variants = possible_discriminants.popitem()
    discriminant_name = camel(discriminant_json_name, capitalize=True)
    
    variants = []
    variants_by_tag = []
    parent = UnionByField(name, [], variants, discriminant_json_name, variants_by_tag)

    for tag_values, schema in discriminant_variants:
        properties, inner_types, embed_ref = extract_object(schema)

        properties = [p for p in properties if p.json_name != discriminant_json_name]

        add_variant_to = variants
        variant_properties = properties
        variant_inner_types = inner_types
        variant_embed_ref = embed_ref
        variant_parent = parent

        common_class = None

        common_class_name = extract_directive(schema, "common_class")
        if common_class_name is not None:
            common_class = Class(
                camel(common_class_name, capitalize=True),
                properties,
                inner_types,
                parent=parent,
                embed_ref=embed_ref,
                sealed=True,
            )
            variants.append(common_class)

            add_variant_to = inner_types
            variant_properties = []
            variant_inner_types = []
            variant_embed_ref = None
            variant_parent = common_class

        for tag_value in tag_values:
            variant_name = discriminant_name
            if tag_value == False:
                variant_name = "Not" + discriminant_name
            elif tag_value != True:
                if (
                    common_class_name is not None
                    and isinstance(tag_value, str)
                    and tag_value.startswith(common_class_name+"_")
                ):
                    tag_value = tag_value[len(common_class_name) + 1:]
                variant_name = camel(tag_value, capitalize=True)

            variant = Class(
                variant_name,
                variant_properties,
                variant_inner_types,
                parent=variant_parent,
                embed_ref=variant_embed_ref,
            )
            add_variant_to.append(variant)

            name_path = variant_name
            if common_class != None:
                name_path = f"{common_class.name}.{name_path}"
            variants_by_tag.append((tag_value, name_path, properties))

    return name, [parent], True

def extract_directive(schema, key):
    comment = schema.get("$comment", "")
    directives_src = re.findall("klaxon\\{([^}]*)\\}", comment)
    directives = {}
    for src in directives_src:
        for kv in src.split(";"):
            k, v = kv.split("=")
            directives[k] = v
    return directives.get(key, None)

def extract_oneof_different_types(schemas, name):
    types = OrderedDict()
    for schema in schemas:
        typ = schema.get("type")
        if typ in types:
            return None, None, False
        types[typ] = schema

    variants = []
    variants_by_type = []
    parent = UnionByType(name, [], variants, variants=variants_by_type)

    for type_, schema in types.items():
        if type_ != "object":
            schema = {
                "type": "object",
                "properties": {"value": schema},
                "required": ["value"],
            }
        variant_name = f"As{camel(type_, capitalize=True)}"
        properties, inner_types, embed_ref = extract_object(schema)
        variant = Class(variant_name, properties, inner_types, parent=parent, embed_ref=embed_ref)
        variants.append(variant)

        variants_by_type.append((type_, f"{parent.name}.{variant_name}"))

    return name, [parent], True

def extract_object(schema):
    required = set(schema.get("required", []))
    fields = schema.get("properties", {})
    fields = OrderedDict((k, fields[k]) for k in schema.get("$propertiesOrder", []))

    for p in required:
        if p not in fields.keys():
            fields[p] = {}

    properties = []
    inner_types = []

    for json_name, field_schema in fields.items():
        kt_name = camel(json_name)

        field_type, field_new_types = extract(field_schema, camel(json_name, capitalize=True))
        inner_types += field_new_types

        default = None
        if json_name not in required:
            if field_type[-1] != "?":
                field_type += "?"
            default = "null"

        properties.append(Property(json_name, kt_name, field_type, default))

    embed_ref = None
    if "$ref" in schema:
        embed_ref = ref_name(schema)

    return properties, inner_types, embed_ref

def camel(s, capitalize=False):
    to = s.split('_')
    skip_cap = 0 if capitalize else 1
    to = to[0:skip_cap] + [
        s.upper() if s in ('id', ) else s.capitalize()
        for s in to[skip_cap:]
    ]
    return ''.join(to)

def emit(schema, package, name):
    declarations = []
    declarations += declarations_from_schema(schema, name)

    definitions = {}
    for def_name, def_schema in schema.get("definitions", {}).items():
        def_name = camel(def_name, capitalize=True)
        def_decls = declarations_from_schema(def_schema, def_name)
        declarations += def_decls
        def_decl = def_decls[0]
        definitions[def_decl.name] = def_decl

    print("// Autogenerated by jsonschema2klaxon.py. DO NOT EDIT.")
    print("")
    print(f"package {package}")
    print("")
    print("import com.beust.klaxon.Json")
    print("import com.beust.klaxon.Klaxon")
    print("import com.beust.klaxon.Converter")
    print("import com.beust.klaxon.JsonValue")
    print("import com.beust.klaxon.JsonObject")
    print("import com.beust.klaxon.KlaxonException")
    print("")

    emitter = Emitter(definitions)
    emitter.emit_declarations(declarations)

    print("")
    print(f"fun {name}.Companion.setUpConverters(klaxon: Klaxon)" + " {")
    for path in emitter.classes_with_converter:
        print(f"    klaxon.converter({'.'.join(path)}.converter(klaxon))")
    print("}")


class Printer:
    def __init__(self):
        self.indent_level = 0

    def indent(self, levels=1):
        p = copy(self)
        p.indent_level += levels
        return p

    def print(self, s):
        spaces = " " * self.indent_level * 4
        print(s.replace("\n", "\n" + spaces), end="")

class Emitter:
    def __init__(self, definitions):
        self.definitions = definitions
        self.classes_with_converter = []

    def emit_declarations(self, decls, printer=Printer(), path=[]):
        for decl in decls:
            printer.print("\n")
            self.emit_decl(decl, printer, path + [decl.name])
            printer.print("\n")

    def emit_decl(self, decl, printer, path):
        if isinstance(decl, Class):
            self.emit_class(decl, printer, path)
        elif isinstance(decl, Enum):
            printer.print(f"enum class {decl.name} " + "{")
            first = True
            for v in decl.variants:
                if not first:
                    printer.print(",\n")
                first = False
                printer.print(f"\n    @Json(name = \"{v}\")\n")
                printer.print(f"    {v.upper()}")
            printer.print("\n}")
        else:
            assert(False)

    def emit_class(self, c, printer, path):
        properties, inner_types = self.resolve_embedded(c)

        if c.sealed:
            printer.print("sealed ")

        printer.print(f"class {c.name}")

        if not c.sealed:
            printer.print("(")
            constructor_args = []

            parents = []
            parent = c.parent
            while parent is not None:
                parents.insert(0, parent)
                parent = parent.parent
            for parent in parents:
                if len(parent.properties) > 0:
                    constructor_args += [(p, 'field_override') for p in parent.properties]
            if len(properties) > 0:
                constructor_args += [(p, 'field') for p in properties]

            if len(constructor_args) > 0:
                self.emit_properties(constructor_args, printer.indent())
                printer.print("\n")
            printer.print(")")

        if c.parent is not None:
            printer.print(f" : {c.parent.name}()")

        printer.print(" {\n")

        if c.sealed:
            self.emit_properties([(p, 'abstract') for p in properties], printer.indent(), True)
        self.emit_declarations(inner_types, printer.indent(), path)

        printer.print("\n    companion object")
        if isinstance(c, Union):
            self.emit_converter(c, printer, path)

        printer.print("\n}")

    def emit_converter(self, c, printer, path):
        printer.print(" {\n")
        self.classes_with_converter.append(path)

        # TODO: The 'enabled' thing is extremely hacky.
        #
        # We're using it to use a single converter for all classes in a union.
        #
        # When we convert from JSON, we decode to an abstract, sealed class, and
        # return a concrete class instance. But when we encode to JSON, we do
        # the opposite: we take an instance of the abstract class, then cast it
        # to the concrete class in order to encode that. This means that
        # canConvert needs to return true for the abstract class and its
        # subclasses, not just the abstract class.
        #
        # But because of this, as soon as we recursively call
        # klaxon.fromJsonObject or klaxon.toJsonString, we go into a loop, since
        # the converter gets invoked again for the concrete class, which
        # canConvert still recognizes. 
        #
        # So before recurring, we just disable the converter, so that Klaxon
        # uses the default converter for this class and its subclasses.
        #
        # This will break recursive types.
        #
        # The right solution would be to declare converters in all subclasses.
        # Those would only implement toJson, while the abstract class converter
        # would only implement fromJson. (Well, actually, Klaxon forces us to
        # implement both. It's one of the various ways its API is bad.)

        printer.print("        fun converter(klaxon: Klaxon) = object : Converter {\n")
        printer.print("             var enabled = true\n")
        printer.print("\n")
        printer.print(f"            override fun canConvert(cls: Class<*>): Boolean =\n")
        printer.print(f"                enabled && {c.name}::class.java.isAssignableFrom(cls)\n")
        print("")

        if isinstance(c, UnionByType):
            printer.print("            override fun fromJson(jv: JsonValue): Any? = try { enabled = false; when {\n")

            for json_type, variant_class in c.variants:
                if json_type == "object":
                    printer.print("                jv.obj != null ->\n")
                    printer.print(f"                    klaxon.fromJsonObject(jv.obj!!, {variant_class}::class.java, {variant_class}::class)\n")
                else:
                    jv_class = None
                    if json_type == "string":
                        jv_class = "String"
                    elif json_type in ["number", "integer"]:
                        jv_class = "Number"
                    elif json_type == "boolean":
                        jv_class = "Boolean"
                    elif json_type == "array":
                        jv_class = "JsonArray<*>"
                    else:
                        assert(False)
                    printer.print(f"                jv.inside is {jv_class} ->\n")
                    printer.print(f"                    klaxon.fromJsonObject(JsonObject(mapOf(\"value\" to jv.inside)), {variant_class}::class.java, {variant_class}::class)\n")

            printer.print(f"                else ->\n")
            printer.print(f"                    throw KlaxonException(\"value with unexpected JSON type: $jv\")\n")
            printer.print("            } } finally { enabled = true }\n")
            print("")
            printer.print(f"            override fun toJson(value: Any): String = " + "try { enabled = false; " + f" when (value as {c.name}) " + "{\n")

            for json_type, variant_class in c.variants:
                printer.print(f"                is {variant_class} ->\n")
                if json_type == "object":
                    printer.print(f"                        klaxon.toJsonString(value as {variant_class})\n")
                else:
                    printer.print(f"                        klaxon.toJsonString((value as {variant_class}).value)\n")
            printer.print("            } } finally { enabled = true }\n")

        elif isinstance(c, UnionByField):
            printer.print(f"            override fun fromJson(jv: JsonValue): Any? = jv.obj!![\"{c.discriminant_json}\"].let " + "{ v ->\n")
            printer.print("                try { enabled = false; when {\n")

            for tag_value, variant_class, _ in c.variants:
                kotlin_comparison = None

                if isinstance(tag_value, bool):
                    kotlin_comparison = f"v == {json.dumps(tag_value)}"
                elif isinstance(tag_value, float) or isinstance(tag_value, int):
                    kotlin_comparison = f"(v as? Number)?.compareTo({tag_value}L) == 0"
                else:
                    kotlin_comparison = f"v == {json.dumps(tag_value)}"

                printer.print(f"                    {kotlin_comparison} ->\n")
                printer.print(f"                        klaxon.fromJsonObject(jv.obj!!, {variant_class}::class.java, {variant_class}::class)\n")

            printer.print(f"                    else ->\n")
            printer.print(f"                        throw KlaxonException(\"value with unexpected value in field \\\"{c.discriminant_json}\\\": $jv\")\n")
            printer.print("                } } finally { enabled = true }\n")
            printer.print("            }\n")
            print("")
            printer.print(f"            override fun toJson(value: Any): String = " + "try { enabled = false; " + f"when (value as {c.name}) " + "{\n")

            for enum_value, variant_class, variant_properties in c.variants:
                printer.print(f"                is {variant_class} ->\n")
                if len(variant_properties) > 0:
                    printer.print(f"                        klaxon.toJsonString(value as {variant_class}).dropLast(1) + " + '",')
                else:
                    # Klaxon transforms empty classes to their toString 🤦‍♂️
                    printer.print('                        "{')
                printer.print('\\"' + c.discriminant_json + '\\":\\"' + enum_value + '\\"}"\n')
            printer.print("           } } finally { enabled = true }\n")

        else:
            assert(False)

        printer.print("        }\n")
        printer.print("    }\n")

    def resolve_embedded(self, c):
        properties, inner_types = copy(c.properties), copy(c.inner_types)
        if c.embed_ref is not None:
            ref = self.definitions[c.embed_ref]
            assert(isinstance(ref, Class))

            ref_properties, ref_inner_types = self.resolve_embedded(ref)
            properties += ref_properties
            inner_types += ref_inner_types
        return properties, inner_types

    def emit_properties(self, properties, printer, abstract=False):
        first = True
        for p, format in properties:
            if not first:
                if not abstract:
                    printer.print(",")
                printer.print("\n")
            first = False

            is_field = format == "field" or format == "field_override"
            if is_field:
                printer.print(f"\n@Json(name = \"{p.json_name}\")")

            printer.print("\n")

            if format == "field_override":
                printer.print("override ")
            if abstract:
                printer.print("abstract ")
            if is_field or abstract:
                printer.print("val ")

            printer.print(f"{p.kt_name}")

            printer.print(f": {p.type}")
            if p.default is not None:
                printer.print(f" = {p.default}")

class OrderedSet:
    def __init__(self, vs):
        self._d = OrderedDict((v, None) for v in vs)

    def union(self, other):
        return OrderedSet(v for v in list(self) + list(other))

    def intersection(self, other):
        return OrderedDict(v for v in list(self) + list(other) if v in self and v in other)

    def __contains__(self, item):
        return item in self._d

    def __iter__(self):
        return self._d.keys().__iter__()

    def __len__(self):
        return len(self._d)

import sys
import json

package = sys.argv[1]
assert(re.match("[a-zA-Z_][a-zA-Z0-9_.]*", package))

name = sys.argv[2]
assert(re.match("[a-zA-Z_][a-zA-Z0-9_]*", name))

emit(json.load(sys.stdin), package, name)
