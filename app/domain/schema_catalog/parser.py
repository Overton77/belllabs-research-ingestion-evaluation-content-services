from __future__ import annotations

from typing import Any

from graphql import GraphQLError, parse, print_ast
from graphql.language.ast import NamedTypeNode
from graphql.utilities import value_from_ast_untyped

from app.domain.schema_catalog.errors import CatalogParseError
from app.domain.schema_catalog.models import (
    Directive,
    FieldDefinition,
    IndexDeclaration,
    InterfaceType,
    ObjectType,
    PhysicalSchemaCatalog,
    RelationshipEndpoint,
    RelationshipType,
)
from app.domain.schema_context.canonicalization import sha256_digest

CATALOG_CORE_GENERATOR_VERSION = "typed-schema-catalog-v1"


def _named_type(node: Any) -> str:
    current = node
    while not isinstance(current, NamedTypeNode):
        current = current.type
    return current.name.value


def _directives(node: Any) -> tuple[Directive, ...]:
    return tuple(
        Directive(
            name=directive.name.value,
            arguments={
                argument.name.value: value_from_ast_untyped(argument.value)
                for argument in directive.arguments or ()
            },
        )
        for directive in node.directives or ()
    )


def _directive(values: tuple[Directive, ...], name: str) -> Directive | None:
    return next((item for item in values if item.name == name), None)


def _field(node: Any) -> FieldDefinition:
    kinds: set[str] = set()
    current = node.type
    while current is not None:
        kinds.add(current.kind)
        current = getattr(current, "type", None)
    return FieldDefinition(
        name=node.name.value,
        description=node.description.value if node.description else None,
        type_expression=print_ast(node.type),
        named_type=_named_type(node.type),
        nullable=node.type.kind != "non_null_type",
        is_list="list_type" in kinds,
        directives=_directives(node),
    )


def _indexes(
    type_name: str, directives: tuple[Directive, ...], directive_name: str
) -> list[IndexDeclaration]:
    result: list[IndexDeclaration] = []
    for directive in directives:
        if directive.name != directive_name:
            continue
        declarations = directive.arguments.get("indexes")
        if not isinstance(declarations, list):
            declarations = [directive.arguments]
        for declaration in declarations:
            if isinstance(declaration, dict):
                result.append(
                    IndexDeclaration(
                        node_type=type_name,
                        directive=directive_name,
                        arguments=declaration,
                    )
                )
    return result


def parse_physical_schema(source: bytes, source_ref: str) -> PhysicalSchemaCatalog:
    """Parse Neo4j GraphQL SDL into a portable, typed physical catalog."""
    try:
        document = parse(source.decode("utf-8"))
    except (UnicodeDecodeError, GraphQLError) as error:
        raise CatalogParseError(
            f"authoritative SDL parse failed: {type(error).__name__}"
        ) from error

    objects: dict[str, ObjectType] = {}
    relationship_property_names: set[str] = set()
    enums: dict[str, tuple[str, ...]] = {}
    unions: dict[str, tuple[str, ...]] = {}
    interfaces: dict[str, InterfaceType] = {}
    other: dict[str, str] = {}
    fulltext: list[IndexDeclaration] = []
    vector: list[IndexDeclaration] = []

    for definition in document.definitions:
        item: Any = definition
        name_node = getattr(definition, "name", None)
        name = name_node.value if name_node else definition.kind
        directives = _directives(definition)
        if definition.kind == "enum_type_definition":
            enums[name] = tuple(value.name.value for value in item.values or ())
        elif definition.kind == "union_type_definition":
            unions[name] = tuple(value.name.value for value in item.types or ())
        elif definition.kind == "interface_type_definition":
            interfaces[name] = InterfaceType(
                name=name,
                description=item.description.value if item.description else None,
                directives=directives,
                fields=tuple(_field(field) for field in item.fields or ()),
                sdl=print_ast(definition),
            )
        elif definition.kind == "object_type_definition":
            fields = tuple(_field(field) for field in item.fields or ())
            aliases = tuple(
                {"field": field.name, **alias.arguments}
                for field in fields
                if (alias := _directive(field.directives, "alias")) is not None
            )
            objects[name] = ObjectType(
                name=name,
                description=item.description.value if item.description else None,
                interfaces=tuple(value.name.value for value in item.interfaces or ()),
                directives=directives,
                fields=fields,
                identity_candidates=tuple(
                    sorted(
                        field.name
                        for field in fields
                        if field.name == "id"
                        or any(value.name in {"id", "unique"} for value in field.directives)
                    )
                ),
                search_candidates=tuple(
                    sorted(
                        field.name
                        for field in fields
                        if field.named_type in {"String", "ID", "Int", "Float", "Date", "DateTime"}
                        and not field.is_list
                    )
                ),
                aliases=aliases,
                sdl=print_ast(definition),
            )
            if _directive(directives, "relationshipProperties") is not None:
                relationship_property_names.add(name)
            fulltext.extend(_indexes(name, directives, "fulltext"))
            vector.extend(_indexes(name, directives, "vector"))
        else:
            other[name] = print_ast(definition)

    relationships: dict[str, list[RelationshipEndpoint]] = {}
    relationship_properties: dict[str, set[str]] = {}
    for declaring_name, object_type in objects.items():
        if declaring_name in relationship_property_names:
            continue
        for field in object_type.fields:
            directive = _directive(field.directives, "relationship")
            if directive is None:
                continue
            relationship_type = str(directive.arguments.get("type") or field.name).upper()
            direction = str(directive.arguments.get("direction", "OUT")).upper()
            if direction not in {"IN", "OUT"}:
                raise CatalogParseError(
                    f"unsupported relationship direction {direction!r} on "
                    f"{declaring_name}.{field.name}"
                )
            physical_start = declaring_name if direction == "OUT" else field.named_type
            physical_end = field.named_type if direction == "OUT" else declaring_name
            endpoint = RelationshipEndpoint(
                element_id=f"relationship-field:{declaring_name}.{field.name}",
                relationship_type=relationship_type,
                declaring_type=declaring_name,
                related_type=field.named_type,
                field_name=field.name,
                direction=direction,
                properties_type=directive.arguments.get("properties"),
                field_type=field.type_expression,
                directives=field.directives,
                physical_start_type=physical_start,
                physical_end_type=physical_end,
            )
            relationships.setdefault(relationship_type, []).append(endpoint)
            if endpoint.properties_type:
                relationship_properties.setdefault(relationship_type, set()).add(
                    endpoint.properties_type
                )

    node_types = {
        name: value for name, value in objects.items() if name not in relationship_property_names
    }
    property_types = {
        name: value for name, value in objects.items() if name in relationship_property_names
    }
    relationship_models = {
        name: RelationshipType(
            name=name,
            endpoints=tuple(
                sorted(
                    endpoints,
                    key=lambda value: (
                        value.declaring_type,
                        value.field_name,
                        value.related_type,
                        value.direction,
                    ),
                )
            ),
            property_types=tuple(sorted(relationship_properties.get(name, set()))),
        )
        for name, endpoints in sorted(relationships.items())
    }
    sorted_fulltext = tuple(
        sorted(fulltext, key=lambda value: (value.node_type, str(value.arguments)))
    )
    sorted_vector = tuple(sorted(vector, key=lambda value: (value.node_type, str(value.arguments))))
    digest_payload = {
        "source_digest": sha256_digest(source),
        "source_bytes": len(source),
        "generator_version": CATALOG_CORE_GENERATOR_VERSION,
        "nodes": {name: value.model_dump(mode="json") for name, value in node_types.items()},
        "relationship_property_types": {
            name: value.model_dump(mode="json") for name, value in property_types.items()
        },
        "relationships": {
            name: value.model_dump(mode="json") for name, value in relationship_models.items()
        },
        "enums": enums,
        "unions": unions,
        "interfaces": {name: value.model_dump(mode="json") for name, value in interfaces.items()},
        "other_definitions": other,
        "fulltext_indexes": [value.model_dump(mode="json") for value in sorted_fulltext],
        "vector_indexes": [value.model_dump(mode="json") for value in sorted_vector],
    }
    return PhysicalSchemaCatalog(
        source_ref=source_ref,
        source_digest=digest_payload["source_digest"],
        source_bytes=digest_payload["source_bytes"],
        generator_version=digest_payload["generator_version"],
        nodes=node_types,
        relationship_property_types=property_types,
        relationships=relationship_models,
        enums=enums,
        unions=unions,
        interfaces=interfaces,
        other_definitions=other,
        catalog_digest=sha256_digest(digest_payload),
        fulltext_indexes=sorted_fulltext,
        vector_indexes=sorted_vector,
    )
