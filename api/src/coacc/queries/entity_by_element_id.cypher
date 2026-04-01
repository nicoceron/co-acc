MATCH (e) WHERE elementId(e) = $element_id
  AND (e:Person OR e:Partner OR e:Company OR e:Contract OR e:Sanction OR e:Election OR e:Amendment
       OR e:Finance OR e:Embargo OR e:Health OR e:Education OR e:Convenio OR e:LaborStats
       OR e:PublicOffice OR e:Bid OR e:DeclaredAsset OR e:Inquiry OR e:SourceDocument
       OR e:Project OR e:JudicialCase OR e:ActoAdministrativo OR e:GacetaTerritorial
       OR e:InquiryRequirement OR e:InquirySession OR e:TVECOrder OR e:EnvironmentalFile)
RETURN e, labels(e) AS entity_labels
LIMIT 1
