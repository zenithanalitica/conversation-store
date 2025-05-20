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
     apoc.coll.toSet(apoc.coll.flatten([p IN paths | nodes(p)])) AS tree_nodes,
     apoc.coll.toSet(apoc.coll.flatten([p IN paths | relationships(p)])) AS tree_rels

// WHERE size(tree_nodes) > 1

RETURN parent, r, root_airline_tweet, tree_nodes, tree_rels, size(tree_nodes) AS num_nodes
// count number of conversations-trees (roots)
// RETURN COUNT(root_airline_tweet) AS conversations, SUM(size(tree_nodes)) AS num_tweets_in_such_conversations
limit 10
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

    # Loop through results and do something with them
    for tweet in records:
        print(tweet)

    # Summary information
    print(
        "The query `{query}` returned {records_count} records in {time} ms.".format(
            query=summary.query,
            records_count=len(records),
            time=summary.result_available_after,
        )
    )
    return records
