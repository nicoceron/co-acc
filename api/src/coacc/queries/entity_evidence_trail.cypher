MATCH (center)
WHERE (
  elementId(center) = $entity_id
  OR elementId(center) = $entity_id_formatted
  OR center.document_id = $entity_id
  OR center.document_id = $entity_id_formatted
  OR center.nit = $entity_id
  OR center.nit = $entity_id_formatted
  OR center.cedula = $entity_id
  OR center.cedula = $entity_id_formatted
  OR center.numero_documento = $entity_id
  OR center.numero_documento = $entity_id_formatted
  OR center.bid_id = $entity_id
  OR center.bid_id = $entity_id_formatted
  OR center.inquiry_id = $entity_id
  OR center.inquiry_id = $entity_id_formatted
  OR center.doc_id = $entity_id
  OR center.doc_id = $entity_id_formatted
)
AND (
  center:Person
  OR center:Partner
  OR center:Company
  OR center:Bid
  OR center:Inquiry
  OR center:SourceDocument
)
CALL {
  WITH center
  OPTIONAL MATCH (center)-[:OFFICER_OF|SOCIO_DE|ADMINISTRA|SAME_AS|POSSIBLY_SAME_AS]->(anchor_company:Company)
  WITH center,
       CASE
         WHEN center:Company THEN [center] + collect(DISTINCT anchor_company)
         ELSE collect(DISTINCT anchor_company)
       END AS anchors
  UNWIND anchors AS anchor
  WITH center, anchor
  WHERE anchor IS NOT NULL
  MATCH (anchor)-[participation:GANO|SUMINISTRO_LICITACAO|LICITO]->(bundle:Bid)
  WITH DISTINCT center, anchor, bundle, type(participation) AS relation_type
  CALL {
    WITH bundle
    OPTIONAL MATCH (bundle)-[doc_rel:REFERENTE_A]->(doc:SourceDocument)
    WITH doc, doc_rel
    ORDER BY
      CASE WHEN doc.uploaded_at IS NOT NULL THEN toString(doc.uploaded_at) ELSE "" END DESC,
      coalesce(doc.title, doc.name, doc.doc_id, "") ASC
    WITH collect(
      CASE
        WHEN doc IS NULL THEN null
        ELSE {
          id: doc.doc_id,
          title: coalesce(doc.title, doc.name, doc.archive_label, doc.contract_reference, doc.doc_id),
          url: CASE WHEN coalesce(doc.document_url, "") <> "" THEN doc.document_url ELSE null END,
          kind: CASE
            WHEN coalesce(doc_rel.document_kind, "") <> "" THEN doc_rel.document_kind
            WHEN coalesce(doc.document_kind, "") <> "" THEN doc.document_kind
            ELSE null
          END,
          extension: CASE WHEN coalesce(doc.document_extension, "") <> "" THEN doc.document_extension ELSE null END,
          uploaded_at: CASE WHEN doc.uploaded_at IS NOT NULL THEN toString(doc.uploaded_at) ELSE null END,
          source: CASE WHEN coalesce(doc.source, "") <> "" THEN doc.source ELSE null END
        }
      END
    ) AS raw_documents,
    collect(DISTINCT CASE
      WHEN coalesce(doc_rel.document_kind, "") <> "" THEN doc_rel.document_kind
      WHEN coalesce(doc.document_kind, "") <> "" THEN doc.document_kind
      ELSE null
    END) AS raw_kinds
    RETURN size([item IN raw_documents WHERE item IS NOT NULL]) AS document_count,
           [item IN raw_documents WHERE item IS NOT NULL][..8] AS documents,
           [kind IN raw_kinds WHERE kind IS NOT NULL][..6] AS document_kinds
  }
  CALL {
    WITH bundle
    OPTIONAL MATCH (buyer:Company)-[:LICITO]->(bundle)
    WITH bundle,
         collect(DISTINCT CASE
           WHEN buyer IS NULL THEN null
           ELSE {
             role: "Comprador",
             name: coalesce(buyer.razon_social, buyer.name, buyer.document_id, buyer.nit),
             document_id: CASE
               WHEN coalesce(buyer.document_id, buyer.nit, "") <> "" THEN coalesce(buyer.document_id, buyer.nit)
               ELSE null
             END,
             entity_id: elementId(buyer)
           }
         END) AS buyer_rows
    OPTIONAL MATCH (supplier:Company)-[:GANO|SUMINISTRO_LICITACAO]->(bundle)
    WITH buyer_rows,
         collect(DISTINCT CASE
           WHEN supplier IS NULL THEN null
           ELSE {
             role: "Proveedor",
             name: coalesce(supplier.razon_social, supplier.name, supplier.document_id, supplier.nit),
             document_id: CASE
               WHEN coalesce(supplier.document_id, supplier.nit, "") <> "" THEN coalesce(supplier.document_id, supplier.nit)
               ELSE null
             END,
             entity_id: elementId(supplier)
           }
         END) AS supplier_rows
    WITH buyer_rows + supplier_rows AS raw_parties
    RETURN [party IN raw_parties WHERE party IS NOT NULL][..6] AS parties
  }
  RETURN bundle.bid_id AS bundle_id,
         "proceso_secop" AS bundle_type,
         coalesce(bundle.name, bundle.procedure_description, bundle.reference, bundle.bid_id) AS title,
         CASE WHEN coalesce(bundle.reference, "") <> "" THEN bundle.reference ELSE null END AS reference,
         CASE
           WHEN coalesce(bundle.procedure_description, "") <> "" THEN bundle.procedure_description
           WHEN coalesce(bundle.name, "") <> "" THEN bundle.name
           ELSE null
         END AS description,
         CASE relation_type
           WHEN "LICITO" THEN "Aparece como comprador público"
           WHEN "GANO" THEN "Aparece como proveedor adjudicado"
           WHEN "SUMINISTRO_LICITACAO" THEN "Aparece como oferente o proveedor"
           ELSE "Aparece en este proceso"
         END AS relation_summary,
         CASE
           WHEN elementId(anchor) <> elementId(center) THEN coalesce(anchor.razon_social, anchor.name, anchor.document_id, anchor.nit)
           ELSE null
         END AS via_entity_name,
         CASE
           WHEN elementId(anchor) <> elementId(center) AND coalesce(anchor.document_id, anchor.nit, "") <> ""
           THEN coalesce(anchor.document_id, anchor.nit)
           ELSE null
         END AS via_entity_ref,
         document_count,
         document_kinds,
         documents,
         parties,
         coalesce(bundle.source, "neo4j_graph") AS source
  UNION
  WITH center
  MATCH (center)-[:REFERENTE_A]->(bundle:Inquiry)
  WITH DISTINCT center, bundle
  CALL {
    WITH bundle
    OPTIONAL MATCH (bundle)-[doc_rel:REFERENTE_A]->(doc:SourceDocument)
    WITH doc, doc_rel
    ORDER BY
      CASE WHEN doc.uploaded_at IS NOT NULL THEN toString(doc.uploaded_at) ELSE "" END DESC,
      coalesce(doc.title, doc.name, doc.doc_id, "") ASC
    WITH collect(
      CASE
        WHEN doc IS NULL THEN null
        ELSE {
          id: doc.doc_id,
          title: coalesce(doc.title, doc.name, doc.archive_label, doc.contract_reference, doc.doc_id),
          url: CASE WHEN coalesce(doc.document_url, "") <> "" THEN doc.document_url ELSE null END,
          kind: CASE
            WHEN coalesce(doc_rel.document_kind, "") <> "" THEN doc_rel.document_kind
            WHEN coalesce(doc.document_kind, "") <> "" THEN doc.document_kind
            ELSE null
          END,
          extension: CASE WHEN coalesce(doc.document_extension, "") <> "" THEN doc.document_extension ELSE null END,
          uploaded_at: CASE WHEN doc.uploaded_at IS NOT NULL THEN toString(doc.uploaded_at) ELSE null END,
          source: CASE WHEN coalesce(doc.source, "") <> "" THEN doc.source ELSE null END
        }
      END
    ) AS raw_documents,
    collect(DISTINCT CASE
      WHEN coalesce(doc_rel.document_kind, "") <> "" THEN doc_rel.document_kind
      WHEN coalesce(doc.document_kind, "") <> "" THEN doc.document_kind
      ELSE null
    END) AS raw_kinds
    RETURN size([item IN raw_documents WHERE item IS NOT NULL]) AS document_count,
           [item IN raw_documents WHERE item IS NOT NULL][..8] AS documents,
           [kind IN raw_kinds WHERE kind IS NOT NULL][..6] AS document_kinds
  }
  CALL {
    WITH bundle, center
    OPTIONAL MATCH (company:Company)-[:REFERENTE_A]->(bundle)
    WITH bundle, center,
         collect(DISTINCT CASE
           WHEN company IS NULL THEN null
           ELSE {
             role: "Entidad relacionada",
             name: coalesce(company.razon_social, company.name, company.document_id, company.nit),
             document_id: CASE
               WHEN coalesce(company.document_id, company.nit, "") <> "" THEN coalesce(company.document_id, company.nit)
               ELSE null
             END,
             entity_id: elementId(company)
           }
         END) AS company_rows
    OPTIONAL MATCH (person:Person)-[:REFERENTE_A]->(bundle)
    WITH center,
         company_rows,
         collect(DISTINCT CASE
           WHEN person IS NULL OR elementId(person) = elementId(center) THEN null
           ELSE {
             role: "Persona relacionada",
             name: coalesce(person.name, person.document_id, person.cedula, person.numero_documento),
             document_id: CASE
               WHEN coalesce(person.document_id, person.cedula, person.numero_documento, "") <> ""
               THEN coalesce(person.document_id, person.cedula, person.numero_documento)
               ELSE null
             END,
             entity_id: elementId(person)
           }
         END) AS person_rows
    WITH company_rows + person_rows AS raw_parties
    RETURN [party IN raw_parties WHERE party IS NOT NULL][..6] AS parties
  }
  RETURN bundle.inquiry_id AS bundle_id,
         "expediente_oficial" AS bundle_type,
         coalesce(bundle.title, bundle.name, bundle.case_title, bundle.reference, bundle.inquiry_id) AS title,
         CASE
           WHEN coalesce(bundle.reference, "") <> "" THEN bundle.reference
           WHEN coalesce(bundle.case_number, "") <> "" THEN bundle.case_number
           ELSE null
         END AS reference,
         CASE
           WHEN coalesce(bundle.summary, "") <> "" THEN bundle.summary
           WHEN coalesce(bundle.description, "") <> "" THEN bundle.description
           WHEN coalesce(bundle.case_title, "") <> "" THEN bundle.case_title
           ELSE null
         END AS description,
         "Aparece en un expediente o boletín oficial" AS relation_summary,
         null AS via_entity_name,
         null AS via_entity_ref,
         document_count,
         document_kinds,
         documents,
         parties,
         coalesce(bundle.source, "neo4j_graph") AS source
}
WITH DISTINCT
  bundle_id,
  bundle_type,
  title,
  reference,
  description,
  relation_summary,
  via_entity_name,
  via_entity_ref,
  document_count,
  document_kinds,
  documents,
  parties,
  source
RETURN bundle_id,
       bundle_type,
       title,
       reference,
       description,
       relation_summary,
       via_entity_name,
       via_entity_ref,
       document_count,
       document_kinds,
       documents,
       parties,
       source
ORDER BY document_count DESC, title ASC
LIMIT toInteger($limit)
