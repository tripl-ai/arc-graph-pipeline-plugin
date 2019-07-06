FROM session.${graphName}
MATCH (n0)-[r]->(n1)
RETURN n0, r, n1