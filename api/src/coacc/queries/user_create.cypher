CREATE (u:User {
  id: $id,
  email: $email,
  password_hash: $password_hash,
  role: coalesce($role, 'reviewer'),
  created_at: datetime()
})
RETURN u.id AS id,
       u.email AS email,
       toString(u.created_at) AS created_at,
       u.role AS role
