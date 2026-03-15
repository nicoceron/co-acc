// Fetch temporal events one hop from entity with cursor-based pagination
// Includes events from SAME_AS equivalent nodes
MATCH (e)
WHERE elementId(e) = $entity_id
  AND (e:Person OR e:Company OR e:Contract OR e:Sanction OR e:Election
       OR e:Amendment OR e:Finance OR e:Embargo OR e:Health OR e:Education
       OR e:Convenio OR e:LaborStats OR e:PublicOffice OR e:Bid OR e:DeclaredAsset
       OR e:Inquiry)
WITH e
OPTIONAL MATCH (e)-[:SAME_AS*1..2]-(other)
WITH e, collect(DISTINCT other) AS others
WITH [e] + others AS equivs
UNWIND equivs AS eq
MATCH (eq)-[r]-(n)
WHERE type(r) <> 'SAME_AS'
  AND (n:Contract OR n:Sanction OR n:Amendment OR n:Election OR n:Finance OR n:Embargo OR n:Convenio
       OR n:Bid OR n:DeclaredAsset OR n:PublicOffice)
WITH DISTINCT n, labels(n) AS lbls,
     COALESCE(n.date, n.date_start, n.date_published, n.publication_date, n.start_date, toString(n.year)) AS event_date
WHERE event_date IS NOT NULL AND event_date <> ''
  AND ($cursor IS NULL OR event_date < $cursor)
RETURN elementId(n) AS id, event_date, lbls, properties(n) AS props
ORDER BY event_date DESC
LIMIT $limit
