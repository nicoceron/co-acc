MATCH (e) WHERE (e.document_key = $id OR e.document_id = $id OR e.nit = $id
            OR e.cedula = $id OR e.numero_documento = $id
            OR e.reps_code = $id
            OR e.contract_id = $id OR e.sanction_id = $id
            OR e.amendment_id = $id
            OR e.finance_id = $id OR e.embargo_id = $id
            OR e.school_id = $id OR e.convenio_id = $id
            OR e.partner_id = $id OR e.bid_id = $id
            OR e.asset_id = $id OR e.office_id = $id
            OR e.doc_id = $id OR e.project_id = $id
            OR e.bpin_code = $id OR e.case_id = $id
            OR e.act_id = $id OR e.gaceta_id = $id
            OR e.requirement_id = $id OR e.session_id = $id
            OR e.order_id = $id OR e.file_id = $id
            OR e.stats_id = $id OR elementId(e) = $id)
  AND (e:Person OR e:Partner OR e:Company OR e:Contract OR e:Sanction OR e:Election
       OR e:Amendment OR e:Finance OR e:Embargo OR e:Health OR e:Education
       OR e:Convenio OR e:LaborStats OR e:PublicOffice OR e:Bid OR e:DeclaredAsset
       OR e:Inquiry OR e:SourceDocument OR e:Project OR e:JudicialCase
       OR e:ActoAdministrativo OR e:GacetaTerritorial OR e:InquiryRequirement
       OR e:InquirySession OR e:TVECOrder OR e:EnvironmentalFile)
RETURN e, labels(e) AS entity_labels
LIMIT 1
