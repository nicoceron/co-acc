CREATE (i:Investigation:Case {
  id: $id,
  title: $title,
  description: $description,
  status: "new",
  created_at: datetime(),
  updated_at: datetime(),
  share_token: null,
  case_scope_updated_at: datetime(),
  case_last_refreshed_at: null,
  case_last_run_id: null,
  case_stale: true
})
WITH i
MATCH (u:User {id: $user_id})
CREATE (u)-[:OWNS]->(i)
RETURN i.id AS id,
       i.title AS title,
       i.description AS description,
       i.status AS status,
       i.created_at AS created_at,
       i.updated_at AS updated_at,
       i.share_token AS share_token,
       [] AS entity_ids
