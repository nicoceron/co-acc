MATCH (u:User {email: $email})
RETURN u.id AS id,
       u.email AS email,
       u.password_hash AS password_hash,
       toString(u.created_at) AS created_at,
       coalesce(u.role, 'reviewer') AS role
