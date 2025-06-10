import logging
import os
from typing import LiteralString, cast

import neo4j
from dotenv import load_dotenv
from neo4j import GraphDatabase

_ = load_dotenv()

URI = cast(str, os.getenv("NEO4J_URI"))
USERNAME = cast(str, os.getenv("NEO4J_USERNAME"))
PASSWORD = cast(str, os.getenv("NEO4J_PASSWORD"))

query: LiteralString = """
MATCH (a_node:Tweet:AirlineTweet)-[r:REPLIES_TO]->(parent:ParentTweet)

CALL apoc.path.spanningTree(a_node, {
  relationshipFilter: '<REPLIES_TO',
  labelFilter: '-AirlineTweet',
  uniqueness: 'NODE_GLOBAL'
}) YIELD path

WITH a_node AS root_airline_tweet, path, parent
WITH root_airline_tweet, collect(path) AS paths, parent

// Flatten paths to get all nodes
WITH root_airline_tweet, parent,
     apoc.coll.toSet(apoc.coll.flatten([p IN paths | nodes(p)])) AS tree_nodes

WHERE size(tree_nodes) > 1

RETURN parent, tree_nodes, root_airline_tweet.airline_id as airline_id
"""


def get_conversations(logger: logging.Logger) -> list[neo4j.Record]:
    with GraphDatabase.driver(URI, auth=(USERNAME, PASSWORD)) as driver:
        driver.verify_connectivity()
        logger.info(f"Connected. Authorization: {driver.verify_authentication()}")
        logger.info("Fetching conversations...")
        return run_query(driver, query, logger)


def run_query(
    driver: neo4j.Driver, query: LiteralString, logger: logging.Logger
) -> list[neo4j.Record]:
    records, summary, _ = driver.execute_query(
        query,
        database_="neo4j",
    )

    # Summary information
    logger.debug(
        f"The query returned {len(records)} records in {summary.result_available_after} ms."
    )
    return records
