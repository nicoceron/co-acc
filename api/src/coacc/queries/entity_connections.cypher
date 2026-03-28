MATCH (center)
WHERE elementId(center) = $entity_id
  AND (center:Person OR center:Partner OR center:Company OR center:Contract OR center:Sanction OR center:Election
       OR center:Amendment OR center:Finance OR center:Embargo OR center:Health OR center:Education
       OR center:Convenio OR center:LaborStats OR center:PublicOffice OR center:Bid
       OR center:DeclaredAsset OR center:Inquiry OR center:Finding)
OPTIONAL MATCH p=(center)-[:SOCIO_DE|DONO_A|CANDIDATO_EM|GANO|CONTRATOU|ADJUDICOU_A|AUTOR_EMENDA|SANCIONADA|TIENE_HALLAZGO|OPERA_UNIDAD|DEVE|RECEBEU_EMPRESTIMO|EMBARGADA|MANTIENE_A|BENEFICIO|GENERO_CONVENIO|CELEBRO_CONVENIO_INTERADMIN|SAME_AS|POSSIBLY_SAME_AS|POSSIBLE_FAMILY_TIE|OFFICER_OF|SUPERVISA_PAGO|SUMINISTRO_LICITACAO|LICITO|DECLARO_BIEN|DECLARO_FINANZAS|RECIBIO_SALARIO|REFERENTE_A|ADMINISTRA*1..4]-(connected)
WHERE length(p) <= $depth
  AND all(x IN nodes(p) WHERE NOT (x:User OR x:Investigation OR x:Annotation OR x:Tag))
WITH center, p
UNWIND CASE WHEN p IS NULL THEN [] ELSE relationships(p) END AS r
WITH DISTINCT center, r, startNode(r) AS src, endNode(r) AS tgt
WHERE coalesce($include_probable, false) OR type(r) NOT IN ["POSSIBLY_SAME_AS", "POSSIBLE_FAMILY_TIE"]
RETURN center AS e,
       r,
       CASE WHEN elementId(src) = elementId(center) THEN tgt ELSE src END AS connected,
       labels(center) AS source_labels,
       CASE WHEN elementId(src) = elementId(center) THEN labels(tgt) ELSE labels(src) END AS target_labels,
       type(r) AS rel_type,
       elementId(startNode(r)) AS source_id,
       elementId(endNode(r)) AS target_id,
       elementId(r) AS rel_id
