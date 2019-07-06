FROM GRAPH session.${graphName0}
MATCH (p:Person)
FROM GRAPH session.${graphName1}
MATCH (c:Customer)
WHERE p.name = c.name
CONSTRUCT ON session.${graphName0}, session.${graphName1}
  CREATE (p)-[:IS]->(c)
RETURN GRAPH