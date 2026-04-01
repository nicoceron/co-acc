MATCH (u:User {id: $user_id})-[:OWNS]->(i:Investigation {id: $investigation_id})
MATCH (e) WHERE (e.document_id = $entity_id OR e.nit = $entity_id
            OR e.cedula = $entity_id OR e.numero_documento = $entity_id
            OR e.reps_code = $entity_id
            OR e.contract_id = $entity_id OR e.sanction_id = $entity_id
            OR e.amendment_id = $entity_id
            OR e.finance_id = $entity_id OR e.embargo_id = $entity_id
            OR e.school_id = $entity_id OR e.convenio_id = $entity_id
            OR e.partner_id = $entity_id OR e.bid_id = $entity_id
            OR e.asset_id = $entity_id OR e.office_id = $entity_id
            OR e.stats_id = $entity_id OR e.project_id = $entity_id
            OR e.bpin_code = $entity_id OR e.case_id = $entity_id
            OR e.act_id = $entity_id OR e.gaceta_id = $entity_id
            OR e.requirement_id = $entity_id OR e.session_id = $entity_id
            OR e.order_id = $entity_id OR e.file_id = $entity_id
            OR e.doc_id = $entity_id OR elementId(e) = $entity_id)
  AND (e:Person OR e:Partner OR e:Company OR e:Contract OR e:Sanction OR e:Election
       OR e:Amendment OR e:Finance OR e:Embargo OR e:Health OR e:Education
       OR e:Convenio OR e:LaborStats OR e:PublicOffice OR e:Bid OR e:DeclaredAsset
       OR e:Inquiry OR e:SourceDocument OR e:Project OR e:JudicialCase
       OR e:ActoAdministrativo OR e:GacetaTerritorial OR e:InquiryRequirement
       OR e:InquirySession OR e:TVECOrder OR e:EnvironmentalFile)
MERGE (i)-[:INCLUDES]->(e)
SET i.updated_at = datetime(),
    i.case_scope_updated_at = datetime(),
    i.case_stale = true
RETURN i.id AS investigation_id,
       coalesce(e.document_id, e.nit, e.cedula, e.numero_documento, e.reps_code, e.partner_id, e.contract_id, e.sanction_id, e.amendment_id, e.finance_id, e.embargo_id, e.school_id, e.convenio_id, e.stats_id, e.bid_id, e.asset_id, e.office_id, e.project_id, e.bpin_code, e.case_id, e.act_id, e.gaceta_id, e.requirement_id, e.session_id, e.order_id, e.file_id, e.doc_id, elementId(e)) AS entity_id
