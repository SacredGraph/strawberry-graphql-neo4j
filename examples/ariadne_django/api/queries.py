from ariadne import QueryType
from strawberry_graphql_neo4j import neo4j_graphql

query = QueryType()


@query.field("Movie")
@query.field("MoviesByYear")
def resolve(obj, info, **kwargs):
    return neo4j_graphql(obj, info.context, info, **kwargs)
