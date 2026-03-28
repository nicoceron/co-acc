# Real-Case Validation

Date: 2026-03-19

This project now exposes a live validation route for known public cases:

```bash
make validate-known-cases
```

or directly:

```bash
curl -s 'http://localhost:8000/api/v1/meta/validation/known-cases' | jq '.'
```

## Cases currently checked

1. `FONDECUN` (`900258772`)
   - expected signals:
     - `budget_execution_discrepancy`
     - `public_official_supplier_overlap`
   - current live metrics:
     - `4` contracts
     - `COP 32,854,689,276` contracted value
     - `1` execution-gap contract
     - `COP 2,142,616,823` invoiced in the gap
     - `1` linked public official
   - public references:
     - https://www.integracionsocial.gov.co/index.php/noticias/116-otras-noticias/5795-contraloria-distrital-reconoce-a-integracion-social-los-avances-en-la-construccion-del-centro-dia-campo-verde
     - https://www.procuraduria.gov.co/Pages/procuraduria-alerta-ejecucion-3-billones-de-19-contrataderos.aspx
     - https://www.elespectador.com/bogota/bosa-espera-recuperar-su-elefante-blanco/

2. `EGOBUS SAS` (`900398793`)
   - expected signal:
     - `sanctioned_supplier_record`
   - current live metrics:
     - `12` sanctions in loaded public sanction records
   - public references:
     - https://bogota.gov.co/mi-ciudad/movilidad/alcaldia-penalosa-empieza-pagarles-propietarios-egobus-y-coobus
     - https://www.transmilenio.gov.co/files/6c7a31a0-0df6-4750-98f5-e1225e1b9583/0e64b507-6aa5-4be4-b90a-f1d3b050fe62/Informe%20de%20gestion%202016-2019.pdf

3. `Vivian del Rosario Moreno Pérez` (`52184154`)
   - expected signal:
     - `candidate_supplier_overlap`
   - current live metrics:
     - `1` candidacy
     - `2` donations
     - `2` supplier contracts
     - `COP 111,240,000` supplier contract value
   - public reference:
     - https://www.procuraduria.gov.co/Pages/cargos-siete-exediles-localidad-bogota-presuntas-irregularidades-conformacion-terna-alcalde-local.aspx

## Notes

- These are validation cases for graph behavior, not legal conclusions.
- The project should describe them as risk signals or public-record overlaps unless there is a final official sanction or conviction explicitly loaded and cited.
- The people watchlist now surfaces:
  - `candidate_supplier_overlap`
  - `donor_supplier_overlap`
  - `disclosure_risk_stack`
