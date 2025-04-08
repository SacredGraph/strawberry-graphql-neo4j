from .utils import (
    cypher_directive_args,
    is_graphql_scalar_type,
    is_array_type,
    inner_type,
    cypher_directive,
    relation_directive,
    inner_filter_params,
    compute_skip_limit,
)


def build_cypher_selection(
    initial, selections, variable_name, schema_type, resolve_info
):
    if len(selections) == 0:
        return initial

    head_selection, *tail_selections = selections

    tail_params = {
        "selections": tail_selections,
        "variable_name": variable_name,
        "schema_type": schema_type,
        "resolve_info": resolve_info,
    }

    field_name = head_selection.name.value
    comma_if_tail = "," if len(tail_selections) > 0 else ""
    # Schema meta fields(__schema, __typename, etc)
    if not schema_type.get_field(field_name):
        return build_cypher_selection(
            initial[1 : initial.rfind(",")] if len(tail_selections) == 0 else initial,
            **tail_params,
        )

    field_type = schema_type.get_field(field_name).type
    inner_schema_type = resolve_info.schema.get_type_by_name(
        getattr(inner_type(field_type), "__name__", None)
    )
    custom_cypher = cypher_directive(schema_type, field_name).get("statement")

    # Database meta fields(_id)
    if field_name == "_id":
        return build_cypher_selection(
            f"{initial}{field_name}: ID({variable_name}){comma_if_tail}", **tail_params
        )

    # We have a graphql object type
    nested_variable = variable_name + "_" + field_name
    skip_limit = compute_skip_limit(head_selection, resolve_info.variable_values)
    nested_params = {
        "initial": "",
        "selections": getattr(head_selection.selection_set, "selections", []),
        "variable_name": nested_variable,
        "schema_type": inner_schema_type,
        "resolve_info": resolve_info,
    }

    # Main control flow
    if not is_array_type(field_type):
        if custom_cypher:
            return build_cypher_selection(
                (
                    f'{initial}{field_name}: apoc.cypher.runFirstColumnSingle("{custom_cypher}", '
                    f"{cypher_directive_args(variable_name, head_selection, schema_type, resolve_info, custom_cypher)})"
                    f"{comma_if_tail}"
                ),
                **tail_params,
            )

        rel = relation_directive(schema_type, field_name)
        rel_type = rel.get("name")
        rel_direction = rel.get("direction")
        subquery_args = inner_filter_params(head_selection)

        if not (rel_type is None):
            var = f"{initial}{field_name}: {'head(' if not is_array_type(field_type) else ''}"
            var += f"[({variable_name}){'<' if rel_direction in ['in', 'IN'] else ''}"
            var += f"-[:{rel_type}]-{'>' if rel_direction in ['out', 'OUT'] else ''}"
            var += f"({nested_variable}:{inner_schema_type.origin.__name__} {subquery_args}) | {nested_variable} "
            
            cypher_selection = build_cypher_selection(**nested_params)
            if cypher_selection:
                var += f"{{{cypher_selection}}}]"
            
            var += f"{')' if not is_array_type(field_type) else ''}{skip_limit} {comma_if_tail}"

            return build_cypher_selection(
                var,
                **tail_params,
            )

        # graphql scalar type, no custom cypher statement
        return build_cypher_selection(
            f"{initial} .{field_name} {comma_if_tail}", **tail_params
        )

    if custom_cypher:
        # similar: [ x IN apoc.cypher.runFirstColumnMany("WITH {this} AS this MATCH (this)--(:Genre)--(o:Movie)
        # RETURN o", {this: movie}, true) |x {.title}][1..2])

        field_is_list = not not getattr(field_type, "of_type", None)

        var = f'{initial}{field_name}: {"" if field_is_list else "head("}'
        var += f'[ {nested_variable} IN apoc.cypher.runFirstColumnMany("{custom_cypher}", '
        var += f"{cypher_directive_args(variable_name, head_selection, schema_type, resolve_info, custom_cypher)}) | {nested_variable} "
        
        cypher_selection = build_cypher_selection(**nested_params)
        if cypher_selection:
            var += f"{{{cypher_selection}}}"
        
        var += f"]{')' if not field_is_list else ''}{skip_limit} {comma_if_tail}"

        return build_cypher_selection(
            var,
            **tail_params,
        )

    # graphql object type, no custom cypher

    rel = relation_directive(schema_type, field_name)
    rel_type = rel.get("name")
    rel_direction = rel.get("direction")
    subquery_args = inner_filter_params(head_selection)

    if rel_type is None:
        cypher_selection = build_cypher_selection( initial='', selections=getattr(head_selection.selection_set, 'selections', []), variable_name=field_name, schema_type=inner_schema_type, resolve_info=resolve_info)

        var = f"{initial} {field_name}: {'head(' if not is_array_type(field_type) else ''}[{field_name} in {variable_name}.{field_name} | {field_name} "
        
        if cypher_selection:
            var += f"{{{ cypher_selection  }}}"
            
        var += f"]{')' if not is_array_type(field_type) else ''}{skip_limit} {comma_if_tail}"
            
        return build_cypher_selection(
            var,
            **tail_params,
        )

    cypher_selection = build_cypher_selection(**nested_params)

    var = f"{initial}{field_name}: {'head(' if not is_array_type(field_type) else ''}"
    var += f"[({variable_name}){'<' if rel_direction in ['in', 'IN'] else ''}"
    var += f"-[:{rel_type}]-{'>' if rel_direction in ['out', 'OUT'] else ''}"
    var += f"({nested_variable}:{inner_schema_type.origin.__name__} {subquery_args}) | {nested_variable} "
    if cypher_selection:
        var += f"{{{cypher_selection}}}"
    var += f"{')' if not is_array_type(field_type) else ''}{skip_limit} {comma_if_tail}"

    return build_cypher_selection(
        var,
        **tail_params,
    )
