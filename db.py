from typing import LiteralString
import neo4j
import credentials
from neo4j import GraphDatabase

query: LiteralString = """
MATCH (a_node:Tweet:AirlineTweet)-[r:REPLIES_TO]->(parent:ParentTweet)

CALL apoc.path.spanningTree(a_node, {
  relationshipFilter: '<REPLIES_TO',
  labelFilter: '-AirlineTweet',
  uniqueness: 'NODE_GLOBAL'
}) YIELD path

WITH a_node AS root_airline_tweet, path, parent, r
WITH root_airline_tweet, collect(path) AS paths, parent, r

// Flatten paths to get all nodes and relationships
WITH root_airline_tweet, parent, r,
     apoc.coll.toSet(apoc.coll.flatten([p IN paths | nodes(p)])) AS tree_nodes

RETURN parent, tree_nodes, size(tree_nodes+1) AS num_nodes
"""


def get_conversations():
    with GraphDatabase.driver(
        credentials.uri, auth=(credentials.user, credentials.password)
    ) as driver:
        driver.verify_connectivity()
        print(f"Auth: {driver.verify_authentication()}")
        return run_query(driver, query)


def run_query(driver: neo4j.Driver, query: LiteralString) -> list[neo4j.Record]:
    records, summary, _ = driver.execute_query(
        query,
        database_="neo4j",
    )

    # Summary information
    print(
        f"The query returned {len(records)} records in {summary.result_available_after} ms."
    )
    return records
