MATCH (n)
WHERE elementId(n) = $entity_id
  AND (n:Person OR n:Company OR n:Contract OR n:Sanction OR n:Election
       OR n:Amendment OR n:Finance OR n:Embargo OR n:Health OR n:Education
       OR n:Convenio OR n:LaborStats OR n:PublicOffice OR n:Bid OR n:DeclaredAsset
       OR n:Inquiry)
RETURN COUNT { (n)--() } AS degree
