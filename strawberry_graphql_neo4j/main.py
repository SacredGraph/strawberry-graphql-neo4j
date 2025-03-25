import re
import json
import logging
from pydash import filter_
from .selections import build_cypher_selection
from .utils import (
    is_mutation,
    is_add_relationship_mutation,
    type_identifiers,
    low_first_letter,
    cypher_directive,
    mutation_meta_directive,
    extract_query_result,
    extract_selections,
    fix_params_for_add_relationship_mutation,
    is_array_type,
)
from collections.abc import Iterable
from dataclasses import fields

logger = logging.getLogger("neo4j_graphql_py")
logger.setLevel(logging.DEBUG)
ch = logging.StreamHandler()
formatter = logging.Formatter("%(levelname)s:     %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)


def neo4j_graphql(obj, context, resolve_info, debug=False, **kwargs):
    if is_mutation(resolve_info):
        query = cypher_mutation(context, resolve_info, **kwargs)
        if is_add_relationship_mutation(resolve_info):
            # kwargs = fix_params_for_add_relationship_mutation(resolve_info, **kwargs)
            pass
        else:
            kwargs = {"params": kwargs}
    else:
        query = cypher_query(context, resolve_info, **kwargs)

    if debug:
        logger.info(query)
        logger.info(kwargs)

    with context.get("driver").session() as session:
        data = session.run(query, **kwargs)
        data = extract_query_result(data, resolve_info.return_type)

        def initialize_type(type_def, value):
            if isinstance(value, dict):
                # Recursively initialize nested dictionaries
                initialized_dict = {}

                klass = type_def

                if getattr(type_def, "origin", None):
                    klass = type_def.origin

                if getattr(type_def, "of_type", None):
                    klass = type_def.of_type

                for k, v in value.items():
                    if any([k == field.name for field in fields(klass)]):
                        if isinstance(v, (dict, list)):
                            field_type = type_def.get_field(k).type
                            if is_array_type(field_type):
                                field_type = field_type.of_type
                            initialized_dict[k] = initialize_type(field_type, v)
                        else:
                            initialized_dict[k] = v

                return klass(**initialized_dict)
            elif isinstance(value, list):
                # Handle lists by recursively initializing each item
                return [initialize_type(type_def, item) for item in value]
            return value

        type_def = (
            resolve_info.return_type.of_type
            if getattr(resolve_info.return_type, "of_type", None)
            else resolve_info.return_type
        )
        return initialize_type(
            resolve_info.schema.get_type_by_name(type_def.__name__), data
        )


def cypher_query(context, resolve_info, first=-1, offset=0, _id=None, **kwargs):
    types_ident = type_identifiers(resolve_info.return_type)
    type_name = types_ident.get("type_name")
    variable_name = types_ident.get("variable_name")
    schema_type = resolve_info.schema.get_type_by_name(type_name)

    filtered_field_nodes = filter_(
        resolve_info.field_nodes, lambda n: n.name.value == resolve_info.field_name
    )

    # FIXME: how to handle multiple field_node matches
    selections = extract_selections(
        filtered_field_nodes[0].selection_set.selections, []
    )  # resolve_info.fragments)

    # if len(selections) == 0:
    #     # FIXME: why aren't the selections found in the filteredFieldNode?
    #     selections = extract_selections(resolve_info.operation.selection_set.selections, resolve_info.fragments)

    print(f"kwargs: {kwargs}")

    # FIXME: support IN for multiple values -> WHERE
    arg_string = re.sub(r"\"([^(\")]+)\":", "\\1:", json.dumps(kwargs))

    id_where_predicate = f"WHERE ID({variable_name})={_id} " if _id is not None else ""
    outer_skip_limit = f'SKIP {offset}{" LIMIT " + str(first) if first > -1 else ""}'

    cyp_dir = cypher_directive(
        resolve_info.schema.get_type_by_name("Query"), resolve_info.field_name
    )
    if cyp_dir:
        custom_cypher = cyp_dir.get("statement")
        query = (
            f'WITH apoc.cypher.runFirstColumnMany("{custom_cypher}", {arg_string}) AS x '
            f"UNWIND x AS {variable_name} RETURN {variable_name} "
            f'{{{build_cypher_selection("", selections, variable_name, schema_type, resolve_info)}}} '
            f"AS {variable_name} {outer_skip_limit}"
        )
    else:
        # No @cypher directive on QueryType
        query = f"MATCH ({variable_name}:{type_name} {arg_string}) {id_where_predicate}"
        query += (
            f"RETURN {variable_name} "
            f'{{{build_cypher_selection("", selections, variable_name, schema_type, resolve_info)}}}'
            f" AS {variable_name} {outer_skip_limit}"
        )

    return query


def cypher_mutation(context, resolve_info, first=-1, offset=0, _id=None, **kwargs):
    # FIXME: lots of duplication here with cypherQuery, extract into util module
    types_ident = type_identifiers(resolve_info.return_type)
    type_name = types_ident.get("type_name")
    variable_name = types_ident.get("variable_name")
    schema_type = resolve_info.schema.get_type_by_name(type_name)

    filtered_field_nodes = filter_(
        resolve_info.field_nodes, lambda n: n.name.value == resolve_info.field_name
    )

    # FIXME: how to handle multiple field_node matches
    selections = extract_selections(
        getattr(filtered_field_nodes[0].selection_set, "selections", []),
        getattr(resolve_info, "fragments", []),
    )

    def custom_json(obj):
        return getattr(obj, "__dict__", obj)

    # FIXME: support IN for multiple values -> WHERE
    arg_string = re.sub(
        r"\"([^(\")]+)\":", "\\1:", json.dumps(kwargs, default=custom_json)
    )

    id_where_predicate = f"WHERE ID({variable_name})={_id} " if _id is not None else ""
    outer_skip_limit = f'SKIP {offset}{" LIMIT " + str(first) if first > -1 else ""}'

    cyp_dir = cypher_directive(
        resolve_info.schema.get_type_by_name("Mutation"), resolve_info.field_name
    )
    if cyp_dir:
        custom_cypher = cyp_dir.get("statement")
        query = (
            f'CALL apoc.cypher.doIt("{custom_cypher}", {arg_string}) YIELD value '
            f"WITH apoc.map.values(value, [keys(value)[0]])[0] AS {variable_name} "
            f'RETURN {variable_name} {{{build_cypher_selection("", selections, variable_name, schema_type, resolve_info)}}} '
            f"AS {variable_name} {outer_skip_limit}"
        )
    # No @cypher directive on MutationType
    elif resolve_info.field_name.startswith(
        "create"
    ) or resolve_info.field_name.startswith("Create"):
        # Create node
        # TODO: handle for create relationship
        # TODO: update / delete
        # TODO: augment schema
        query = (
            f"CREATE ({variable_name}:{type_name}) SET {variable_name} = $params RETURN {variable_name} "
            f'{{{build_cypher_selection("", selections, variable_name, schema_type, resolve_info)}}} '
            f"AS {variable_name}"
        )
    elif resolve_info.field_name.startswith(
        "add"
    ) or resolve_info.field_name.startswith("Add"):
        mutation_meta = mutation_meta_directive(
            resolve_info.schema.get_type_by_name("Mutation"), resolve_info.field_name
        )
        relation_name = mutation_meta.get("relationship")
        from_type = mutation_meta.get("from")
        from_var = low_first_letter(from_type)
        to_type = mutation_meta.get("to")
        to_var = low_first_letter(to_type)
        from_param = (
            resolve_info.schema.get_type_by_name("Mutation").fields[resolve_info.field_name]
            .ast_node.arguments[0]
            .name.value[len(from_var) :]
        )
        to_param = (
            resolve_info.schema.get_type_by_name("Mutation").fields[resolve_info.field_name]
            .ast_node.arguments[1]
            .name.value[len(to_var) :]
        )
        query = (
            f"MATCH ({from_var}:{from_type} {{{from_param}: "
            f"${resolve_info.schema.get_type_by_name("Mutation").fields[resolve_info.field_name].ast_node.arguments[0].name.value}}}) "
            f"MATCH ({to_var}:{to_type} {{{to_param}: "
            f"${resolve_info.schema.get_type_by_name("Mutation").fields[resolve_info.field_name].ast_node.arguments[1].name.value}}}) "
            f"CREATE ({from_var})-[:{relation_name}]->({to_var}) "
            f"RETURN {from_var} "
            f'{{{build_cypher_selection("", selections, variable_name, schema_type, resolve_info)}}} '
            f"AS {from_var}"
        )
    else:
        raise Exception("Mutation does not follow naming conventions")
    return query


def augment_schema(schema):
    from .augment_schema import add_mutations_to_schema

    mutation_schema = add_mutations_to_schema(schema)
    return mutation_schema
