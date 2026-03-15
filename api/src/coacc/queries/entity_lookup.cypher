MATCH (e)
WHERE (
    e:Person AND (
        e.document_id = $identifier
        OR e.document_id = $identifier_formatted
        OR e.cedula = $identifier
        OR e.cedula = $identifier_formatted
        OR e.numero_documento = $identifier
        OR e.numero_documento = $identifier_formatted
        OR e.cpf = $identifier
        OR e.cpf = $identifier_formatted
    )
)
   OR (
    e:Company AND (
        e.document_id = $identifier
        OR e.document_id = $identifier_formatted
        OR e.nit = $identifier
        OR e.nit = $identifier_formatted
        OR e.cnpj = $identifier
        OR e.cnpj = $identifier_formatted
    )
)
RETURN e, labels(e) AS entity_labels, elementId(e) AS entity_id
LIMIT 1
