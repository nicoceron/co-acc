from __future__ import annotations

from typing import TYPE_CHECKING

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from coacc_etl.curated import CuratedBuildError, build_curated

if TYPE_CHECKING:
    from pathlib import Path


def _write_rows(root: Path, source: str, rows: list[dict[str, object]]) -> None:
    out = root / "raw" / f"source={source}" / "year=2026" / "month=05"
    out.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(rows), out / "fixture.parquet")


def _exposed_position_feature_table() -> str:
    return "table=signal_feature_procurement_politically_exposed_position_supplier_overlap"


def test_build_curated_procurement_signal_uses_catalog_aliases(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_rows(
        tmp_path,
        "jbjy-vk9h",
        [
            {
                "contract_id": "C-1",
                "contract_reference": "REF-1",
                "procurement_process": "P-1",
                "process_url": "{'url': 'https://secop.example/C-1'}",
                "supplier_document": "900123456-8",
                "supplier_doc_type": "NIT",
                "awarded_supplier": "Proveedor Sancionado SAS",
                "entity_nit": "800111222",
                "entity_name": "Comprador Uno",
                "department": "NARINO",
                "city": "TUMACO",
                "sector": "Salud",
                "procurement_modality": "Contratacion directa",
                "contract_type": "Prestacion de servicios",
                "contract_status": "Activo",
                "enables_advance_payment": "No",
                "liquidation": "No",
                "contract_value": "125000000",
                "advance_payment_value": "0",
                "invoiced_value": "0",
                "pending_payment_value": "0",
                "paid_value": "0",
                "amortized_value": "0",
                "pending_value": "0",
                "pending_execution_value": "0",
                "added_days": "0",
                "signing_date": "2026-05-01",
                "contract_start_date": "2026-05-02",
                "contract_end_date": "2026-12-31",
                "last_update": "2026-05-03T00:00:00",
            },
            {
                "contract_id": "C-2",
                "contract_reference": "REF-2",
                "procurement_process": "P-2",
                "process_url": "{'url': 'https://secop.example/C-2'}",
                "supplier_document": "1001234567",
                "supplier_doc_type": "CC",
                "awarded_supplier": "Persona No Nit",
                "entity_nit": "800111222",
                "entity_name": "Comprador Uno",
                "department": "NARINO",
                "city": "TUMACO",
                "sector": "Salud",
                "procurement_modality": "Licitacion",
                "contract_type": "Suministro",
                "contract_value": "45000000",
                "signing_date": "2026-05-01",
                "contract_start_date": "2026-05-02",
                "contract_end_date": "2026-12-31",
                "last_update": "2026-05-03T00:00:00",
            },
            {
                "contract_id": "CV-1",
                "contract_reference": "REF-V-1",
                "procurement_process": "PV-1",
                "process_url": "https://secop.example/CV-1",
                "supplier_document": "902000111-1",
                "supplier_doc_type": "NIT",
                "awarded_supplier": "Proveedor Valor Alto SAS",
                "entity_nit": "801222333",
                "entity_name": "Comprador Valor Alto",
                "department": "BOGOTA",
                "city": "BOGOTA",
                "sector": "Infraestructura",
                "procurement_modality": "Contratación directa",
                "contract_type": "Obra",
                "contract_value": "2000000000",
                "signing_date": "2026-05-05",
                "contract_start_date": "2026-05-06",
                "contract_end_date": "2026-12-31",
                "last_update": "2026-05-07T00:00:00",
            },
            {
                "contract_id": "CPAY-1",
                "contract_reference": "REF-PAY-1",
                "procurement_process": "PPAY-1",
                "process_url": "https://secop.example/CPAY-1",
                "supplier_document": "907000113-4",
                "supplier_doc_type": "NIT",
                "awarded_supplier": "Proveedor Pago Anomalo SAS",
                "entity_nit": "800111222",
                "entity_name": "Comprador Uno",
                "department": "BOGOTA",
                "city": "BOGOTA",
                "sector": "Pago QA",
                "procurement_modality": "Licitacion publica",
                "contract_type": "Servicios",
                "contract_status": "En ejecucion",
                "enables_advance_payment": "Si",
                "liquidation": "No",
                "contract_value": "1000000000",
                "advance_payment_value": "600000000",
                "invoiced_value": "0",
                "pending_payment_value": "1000000000",
                "paid_value": "0",
                "amortized_value": "0",
                "pending_value": "1000000000",
                "pending_execution_value": "1000000000",
                "signing_date": "2026-05-08",
                "contract_start_date": "2026-05-09",
                "contract_end_date": "2026-12-31",
                "last_update": "2026-05-10T00:00:00",
            },
            {
                "contract_id": "CSUSP-1",
                "contract_reference": "REF-SUSP-1",
                "procurement_process": "PSUSP-1",
                "process_url": "https://secop.example/CSUSP-1",
                "supplier_document": "908000113-8",
                "supplier_doc_type": "NIT",
                "awarded_supplier": "Proveedor Suspendido SAS",
                "entity_nit": "800111222",
                "entity_name": "Comprador Uno",
                "department": "BOGOTA",
                "city": "BOGOTA",
                "sector": "Suspension QA",
                "procurement_modality": "Licitacion publica",
                "contract_type": "Obra",
                "contract_status": "Suspendido",
                "enables_advance_payment": "No",
                "liquidation": "No",
                "contract_value": "250000000",
                "advance_payment_value": "0",
                "invoiced_value": "0",
                "pending_payment_value": "250000000",
                "paid_value": "0",
                "amortized_value": "0",
                "pending_value": "250000000",
                "pending_execution_value": "250000000",
                "added_days": "60",
                "signing_date": "2026-04-01",
                "contract_start_date": "2026-04-02",
                "contract_end_date": "2026-12-31",
                "last_update": "2026-06-10T00:00:00",
            },
            *[
                {
                    "contract_id": f"CX-{index}",
                    "contract_reference": f"REF-X-{index}",
                    "procurement_process": f"PX-{index}",
                    "process_url": f"https://secop.example/CX-{index}",
                    "supplier_document": "900765432-6",
                    "supplier_doc_type": "NIT",
                    "awarded_supplier": "Proveedor Concentrado SAS",
                    "entity_nit": f"800{index:06d}",
                    "entity_name": f"Comprador {index}",
                    "department": "NARINO" if index % 2 else "CAUCA",
                    "city": "TUMACO",
                    "sector": "Infraestructura",
                    "procurement_modality": "Licitacion",
                    "contract_type": "Obra",
                    "contract_value": "100000000",
                    "signing_date": "2026-05-01",
                    "contract_start_date": "2026-05-02",
                    "contract_end_date": "2026-12-31",
                    "last_update": "2026-05-03T00:00:00",
                }
                for index in range(50)
            ],
            *[
                {
                    "contract_id": f"CR-{index}",
                    "contract_reference": f"REF-R-{index}",
                    "procurement_process": f"PR-{index}",
                    "process_url": f"https://secop.example/CR-{index}",
                    "supplier_document": "901000111-8",
                    "supplier_doc_type": "NIT",
                    "awarded_supplier": "Proveedor Recurrente SAS",
                    "entity_nit": "800999888",
                    "entity_name": "Comprador Recurrente",
                    "department": "ANTIOQUIA",
                    "city": "MEDELLIN",
                    "sector": "Tecnologia",
                    "procurement_modality": "Contratacion directa",
                    "contract_type": "Servicios",
                    "contract_value": "200000000",
                    "signing_date": "2026-05-01",
                    "contract_start_date": "2026-05-02",
                    "contract_end_date": "2026-12-31",
                    "last_update": "2026-05-03T00:00:00",
                }
                for index in range(10)
            ],
            *[
                {
                    "contract_id": f"CN-{buyer_index}-{contract_index}",
                    "contract_reference": f"REF-N-{buyer_index}-{contract_index}",
                    "procurement_process": f"PN-{buyer_index}-{contract_index}",
                    "process_url": (
                        f"https://secop.example/CN-{buyer_index}-{contract_index}"
                    ),
                    "supplier_document": "906000111-6",
                    "supplier_doc_type": "NIT",
                    "awarded_supplier": "Proveedor Red Densa SAS",
                    "entity_nit": f"DENSE-BUYER-{buyer_index}",
                    "entity_name": f"Comprador Red Densa {buyer_index}",
                    "department": "VALLE DEL CAUCA",
                    "city": "CALI",
                    "sector": "Tecnologia",
                    "procurement_modality": "Licitacion publica",
                    "contract_type": "Servicios",
                    "contract_value": "100000000",
                    "signing_date": "2026-05-01",
                    "contract_start_date": "2026-05-02",
                    "contract_end_date": "2026-12-31",
                    "last_update": "2026-05-03T00:00:00",
                }
                for buyer_index in range(10)
                for contract_index in range(5)
            ],
            *[
                {
                    "contract_id": f"CO-{index}",
                    "contract_reference": f"REF-O-{index}",
                    "procurement_process": f"PO-{index}",
                    "process_url": f"https://secop.example/CO-{index}",
                    "supplier_document": f"700{index:06d}",
                    "supplier_doc_type": "CC",
                    "awarded_supplier": f"Proveedor Peer Outlier {index}",
                    "entity_nit": "800111222",
                    "entity_name": "Comprador Uno",
                    "department": "META",
                    "city": "VILLAVICENCIO",
                    "sector": "Transporte Outlier QA",
                    "procurement_modality": "Selección Abreviada Outlier QA",
                    "contract_type": "Consultoría QA",
                    "contract_value": "100000000",
                    "signing_date": "2026-05-01",
                    "contract_start_date": "2026-05-02",
                    "contract_end_date": "2026-12-31",
                    "last_update": "2026-05-03T00:00:00",
                }
                for index in range(100)
            ],
            {
                "contract_id": "CO-OUTLIER-1",
                "contract_reference": "REF-O-OUTLIER-1",
                "procurement_process": "PO-OUTLIER-1",
                "process_url": "https://secop.example/CO-OUTLIER-1",
                "supplier_document": "799999999",
                "supplier_doc_type": "CC",
                "awarded_supplier": "Proveedor Outlier SAS",
                "entity_nit": "800111222",
                "entity_name": "Comprador Uno",
                "department": "META",
                "city": "VILLAVICENCIO",
                "sector": "Transporte Outlier QA",
                "procurement_modality": "Selección Abreviada Outlier QA",
                "contract_type": "Consultoría QA",
                "contract_value": "5000000000",
                "signing_date": "2026-05-01",
                "contract_start_date": "2026-05-02",
                "contract_end_date": "2026-12-31",
                "last_update": "2026-05-03T00:00:00",
            },
        ],
    )
    _write_rows(
        tmp_path,
        "paco_sanctions",
        [
            {
                "source_id": "paco-1",
                "raw_paco_record_id": "paco-1",
                "paco_feed": "responsabilidades_fiscales",
                "source_url": "https://paco.example/1",
                "subject_document_id": "900123456",
                "subject_name": "Proveedor Sancionado SAS",
                "subject_type": "PERSONA JURIDICA",
                "sanction_type": "Responsabilidad fiscal",
                "sanction_date": "2025-12-31",
                "reference": "RF-1",
                "contract_id": "",
                "amount": "5000000",
                "affected_entity": "Comprador Uno",
                "raw_record_json": "{}",
            }
        ],
    )
    _write_rows(
        tmp_path,
        "u99c-7mfm",
        [
            {
                "id_contrato": "CSUSP-1",
                "tipo": "Suspension",
                "fecha_de_creacion": "2026-05-10T00:00:00",
                "fecha_de_aprobacion": "2026-05-11T00:00:00",
                "proposito_de_la_modificacion": "Acta de suspension inicial",
                "fecha_de_inicio_del_contrato": "2026-05-11",
                "fecha_de_fin_del_contrato": "2026-06-10",
            },
            {
                "id_contrato": "CSUSP-1",
                "tipo": "Suspension",
                "fecha_de_creacion": "2026-06-10T00:00:00",
                "fecha_de_aprobacion": "2026-06-11T00:00:00",
                "proposito_de_la_modificacion": "Prorroga de suspension",
                "fecha_de_inicio_del_contrato": "2026-06-11",
                "fecha_de_fin_del_contrato": "2026-07-10",
            },
            {
                "id_contrato": "CSUSP-1",
                "tipo": "Reanudacion",
                "fecha_de_creacion": "2026-07-10T00:00:00",
                "fecha_de_aprobacion": "2026-07-11T00:00:00",
                "proposito_de_la_modificacion": "Reanudacion del contrato",
                "fecha_de_inicio_del_contrato": "2026-07-11",
                "fecha_de_fin_del_contrato": "2026-12-31",
            },
        ],
    )
    _write_rows(
        tmp_path,
        "u8cx-r425",
        [
            {
                "id_contrato": "C-1",
                "identificador_modificacion": "MOD-C-1",
                "identificador": "MOD-C-1",
                "identificador_requerimiento": "REQ-C-1",
                "valor_modificacion": "75000000",
                "dias_extendidos": "15",
                "estado_modificacion": "Aprobada",
                "proposito_modificacion": "Adicionar valor por mayor alcance",
                "descripcion": "Adicion contractual por actividades complementarias",
                "codigo_bpin": "BPIN-MOD-1",
                "fecha_de_aprobacion": "2026-05-15T00:00:00",
                "fecha_version": "2026-05-15T00:00:00",
                "fecha_de_carga": "2026-05-16T00:00:00",
                "fecha_creacion": "2026-05-14T00:00:00",
            }
        ],
    )
    _write_rows(
        tmp_path,
        "mfmm-jqmq",
        [
            {
                "identificadorcontrato": "C-1",
                "tipoejecucion": "Entrega",
                "nombreplan": "Plan de entrega con retraso",
                "fechadeentregaesperada": "2026-05-10T00:00:00",
                "porcentajedeavanceesperado": "100",
                "fechadeentregareal": "2026-06-25T00:00:00",
                "porcentaje_de_avance_real": "100",
                "estado_del_contrato": "En ejecucion",
                "referencia_de_articulos": "ITEM-C-1",
                "descripci_n": "Entrega posterior a la fecha esperada",
                "unidad": "unidad",
                "cantidad_adjudicada": "1",
                "cantidad_planeada": "1",
                "cantidadrecibida": "1",
                "cantidadporrecibir": "0",
                "fechacreacion": "2026-06-26T00:00:00",
            },
            {
                "identificadorcontrato": "C-2",
                "tipoejecucion": "Entrega",
                "nombreplan": "Plan de entrega normal",
                "fechadeentregaesperada": "2026-05-10T00:00:00",
                "porcentajedeavanceesperado": "100",
                "fechadeentregareal": "2026-05-11T00:00:00",
                "porcentaje_de_avance_real": "100",
                "estado_del_contrato": "Finalizado",
                "referencia_de_articulos": "ITEM-C-2",
                "descripci_n": "Entrega dentro de margen",
                "unidad": "unidad",
                "cantidad_adjudicada": "1",
                "cantidad_planeada": "1",
                "cantidadrecibida": "1",
                "cantidadporrecibir": "0",
                "fechacreacion": "2026-05-12T00:00:00",
            },
        ],
    )
    _write_rows(
        tmp_path,
        "c82u-588k",
        [
            {
                ":id": "row-c82u-1",
                "document_id": "902000111-1",
                "identification_class": "NIT",
                "business_name": "Proveedor Valor Alto SAS",
                "matricula": "MAT-1",
                "chamber_of_commerce": "BOGOTA",
                "matricula_status": "ACTIVA",
                "num_identificacion_representante_legal": "123.456.789",
                "clase_identificacion_rl": "CC",
                "representante_legal": "Servidora Publica",
            },
            {
                ":id": "row-c82u-shared-1",
                "document_id": "900765432-6",
                "identification_class": "NIT",
                "business_name": "Proveedor Concentrado SAS",
                "matricula": "MAT-SHARED-1",
                "chamber_of_commerce": "BOGOTA",
                "matricula_status": "ACTIVA",
                "num_identificacion_representante_legal": "222.222.222",
                "clase_identificacion_rl": "CC",
                "representante_legal": "Representante Compartida",
            },
            {
                ":id": "row-c82u-shared-2",
                "document_id": "901000111-8",
                "identification_class": "NIT",
                "business_name": "Proveedor Recurrente SAS",
                "matricula": "MAT-SHARED-2",
                "chamber_of_commerce": "BOGOTA",
                "matricula_status": "ACTIVA",
                "num_identificacion_representante_legal": "222.222.222",
                "clase_identificacion_rl": "CC",
                "representante_legal": "Representante Compartida",
            },
        ],
    )
    _write_rows(
        tmp_path,
        "qmzu-gj57",
        [
            {
                "codigo": "SUP-902",
                "nombre": "Comercial Valor Alto Diferente SAS",
                "nit": "902000111-1",
                "es_entidad": "No",
                "es_grupo": "No",
                "esta_activa": "true",
                "fecha_creacion": "2026-05-01T00:00:00",
                "codigo_categoria_principal": "72100000",
                "descripcion_categoria_principal": "Servicios de construccion",
                "telefono": "6010000000",
                "fax": "",
                "correo": "contacto@example.test",
                "direccion": "Calle 1 2 3",
                "pais": "CO",
                "departamento": "BOGOTA",
                "municipio": "BOGOTA",
                "sitio_web": "https://proveedor.example.test",
                "tipo_empresa": "Persona juridica",
                "nombre_representante_legal": "Representante Diferente",
                "tipo_doc_representante_legal": "CC",
                "n_mero_doc_representante_legal": "987.654.321",
                "telefono_representante_legal": "3000000000",
                "correo_representante_legal": "rep@example.test",
                "espyme": "No",
                "ubicacion": "BOGOTA",
            },
            {
                "codigo": "SUP-900",
                "nombre": "Proveedor Concentrado SAS",
                "nit": "900765432-6",
                "es_entidad": "No",
                "es_grupo": "No",
                "esta_activa": "true",
                "fecha_creacion": "2026-05-01T00:00:00",
                "nombre_representante_legal": "Representante Compartida",
                "tipo_doc_representante_legal": "CC",
                "n_mero_doc_representante_legal": "222.222.222",
            },
        ],
    )
    _write_rows(
        tmp_path,
        "jgra-rz2t",
        [
            {
                "candidate_id": "700111222",
                "cnd_name": "Alcaldia",
                "class_name": "ALCALDE",
                "department_name": "NARINO",
                "municipality_name": "TUMACO",
                "locality_name": "",
                "organization_name": "Comite Ciudadano Uno",
                "candidate_name": "Candidata Uno",
                "cco_id": "CCO-1",
                "tpe_name": "Persona Juridica",
                "person_name": "Proveedor Sancionado SAS",
                "income_amount": "5000000",
                "tid_name": "Nit",
                "income_party_id": "900123456-8",
                "income_act": "ACT-1",
                "tdo_name": "Donacion",
                "party_coalition": "Coalicion Uno",
                "voucher_date": "2019-08-15T00:00:00",
                "income_voucher": "VOUCHER-1",
                "income_concept": "Aporte campana",
            },
            {
                "candidate_id": "700111333",
                "cnd_name": "Concejo",
                "class_name": "CONCEJAL",
                "department_name": "NARINO",
                "municipality_name": "TUMACO",
                "locality_name": "",
                "organization_name": "Comite Control",
                "candidate_name": "Candidata Control",
                "cco_id": "CCO-2",
                "tpe_name": "Persona Juridica",
                "person_name": "Donante No Proveedor SAS",
                "income_amount": "9000000",
                "tid_name": "Nit",
                "income_party_id": "999999999-9",
                "income_act": "ACT-2",
                "tdo_name": "Donacion",
                "party_coalition": "Coalicion Control",
                "voucher_date": "2019-08-20T00:00:00",
                "income_voucher": "VOUCHER-2",
                "income_concept": "Aporte campana",
            },
        ],
    )
    _write_rows(
        tmp_path,
        "p6dx-8zbt",
        [
            {
                "id_del_proceso": "PROC-SHORT-1",
                "id_del_portafolio": "PORT-SHORT-1",
                "referencia_del_proceso": "REF-PROC-SHORT-1",
                "id_adjudicacion": "AWD-SHORT-1",
                "urlproceso": "https://secop.example/PROC-SHORT-1",
                "nit_del_proveedor_adjudicado": "903000111-0",
                "nombre_del_proveedor": "Proveedor Ventana Corta SAS",
                "nit_entidad": "800333444",
                "entidad": "Comprador Ventana Corta",
                "departamento_entidad": "NARINO",
                "ciudad_entidad": "TUMACO",
                "modalidad_de_contratacion": "Licitacion publica",
                "tipo_de_contrato": "Obra",
                "fase": "Presentacion de ofertas",
                "estado_del_procedimiento": "Adjudicado",
                "estado_resumen": "Adjudicado",
                "adjudicado": "Si",
                "valor_total_adjudicacion": "300000000",
                "precio_base": "300000000",
                "conteo_de_respuestas_a_ofertas": "1",
                "respuestas_al_procedimiento": "1",
                "respuestas_externas": "0",
                "proveedores_unicos_con": "1",
                "proveedores_invitados": "3",
                "proveedores_con_invitacion": "3",
                "fecha_de_publicacion_del": "2026-05-01T08:00:00",
                "fecha_de_publicacion_fase_3": "2026-05-01T08:00:00",
                "fecha_de_ultima_publicaci": "2026-05-01T08:00:00",
                "fecha_de_apertura_efectiva": "2026-05-01T08:00:00",
                "fecha_de_apertura_de_respuesta": "2026-05-01T08:00:00",
                "fecha_de_recepcion_de": "2026-05-02T08:00:00",
            },
            {
                "id_del_proceso": "PROC-LONG-1",
                "referencia_del_proceso": "REF-PROC-LONG-1",
                "id_adjudicacion": "AWD-LONG-1",
                "urlproceso": "https://secop.example/PROC-LONG-1",
                "nit_del_proveedor_adjudicado": "904000111-9",
                "nombre_del_proveedor": "Proveedor Ventana Normal SAS",
                "nit_entidad": "800333444",
                "entidad": "Comprador Ventana Corta",
                "departamento_entidad": "NARINO",
                "ciudad_entidad": "TUMACO",
                "modalidad_de_contratacion": "Licitacion publica",
                "tipo_de_contrato": "Obra",
                "fase": "Presentacion de ofertas",
                "estado_del_procedimiento": "Adjudicado",
                "estado_resumen": "Adjudicado",
                "adjudicado": "Si",
                "valor_total_adjudicacion": "300000000",
                "precio_base": "300000000",
                "conteo_de_respuestas_a_ofertas": "4",
                "respuestas_al_procedimiento": "4",
                "respuestas_externas": "0",
                "proveedores_unicos_con": "4",
                "proveedores_invitados": "8",
                "proveedores_con_invitacion": "8",
                "fecha_de_apertura_efectiva": "2026-05-01T08:00:00",
                "fecha_de_apertura_de_respuesta": "2026-05-01T08:00:00",
                "fecha_de_recepcion_de": "2026-05-10T08:00:00",
            },
            *[
                {
                    "id_del_proceso": f"PROC-DROP-R-{index}",
                    "referencia_del_proceso": f"REF-PROC-DROP-R-{index}",
                    "id_adjudicacion": f"AWD-DROP-R-{index}",
                    "urlproceso": f"https://secop.example/PROC-DROP-R-{index}",
                    "nit_del_proveedor_adjudicado": f"905{index:06d}",
                    "nombre_del_proveedor": f"Proveedor Drop Reciente {index}",
                    "nit_entidad": "800555666-1",
                    "entidad": "Comprador Competencia Drop",
                    "departamento_entidad": "NARINO",
                    "ciudad_entidad": "TUMACO",
                    "modalidad_de_contratacion": "Licitacion publica",
                    "tipo_de_contrato": "Servicios",
                    "fase": "Presentacion de ofertas",
                    "estado_del_procedimiento": "Adjudicado",
                    "estado_resumen": "Adjudicado",
                    "adjudicado": "Si",
                    "valor_total_adjudicacion": "150000000",
                    "precio_base": "150000000",
                    "conteo_de_respuestas_a_ofertas": "1",
                    "respuestas_al_procedimiento": "1",
                    "respuestas_externas": "0",
                    "proveedores_unicos_con": "1",
                    "proveedores_invitados": "5",
                    "proveedores_con_invitacion": "5",
                    "fecha_de_publicacion_del": f"2026-05-{index + 1:02d}T08:00:00",
                    "fecha_de_apertura_efectiva": f"2026-05-{index + 1:02d}T08:00:00",
                    "fecha_de_apertura_de_respuesta": f"2026-05-{index + 1:02d}T08:00:00",
                    "fecha_de_recepcion_de": f"2026-05-{index + 10:02d}T08:00:00",
                }
                for index in range(20)
            ],
            *[
                {
                    "id_del_proceso": f"PROC-DROP-P-{index}",
                    "referencia_del_proceso": f"REF-PROC-DROP-P-{index}",
                    "id_adjudicacion": f"AWD-DROP-P-{index}",
                    "urlproceso": f"https://secop.example/PROC-DROP-P-{index}",
                    "nit_del_proveedor_adjudicado": f"906{index:06d}",
                    "nombre_del_proveedor": f"Proveedor Drop Previo {index}",
                    "nit_entidad": "800555666-1",
                    "entidad": "Comprador Competencia Drop",
                    "departamento_entidad": "NARINO",
                    "ciudad_entidad": "TUMACO",
                    "modalidad_de_contratacion": "Licitacion publica",
                    "tipo_de_contrato": "Servicios",
                    "fase": "Presentacion de ofertas",
                    "estado_del_procedimiento": "Adjudicado",
                    "estado_resumen": "Adjudicado",
                    "adjudicado": "Si",
                    "valor_total_adjudicacion": "150000000",
                    "precio_base": "150000000",
                    "conteo_de_respuestas_a_ofertas": "8",
                    "respuestas_al_procedimiento": "8",
                    "respuestas_externas": "0",
                    "proveedores_unicos_con": "8",
                    "proveedores_invitados": "12",
                    "proveedores_con_invitacion": "12",
                    "fecha_de_publicacion_del": f"2025-01-{index + 1:02d}T08:00:00",
                    "fecha_de_apertura_efectiva": f"2025-01-{index + 1:02d}T08:00:00",
                    "fecha_de_apertura_de_respuesta": f"2025-01-{index + 1:02d}T08:00:00",
                    "fecha_de_recepcion_de": f"2025-01-{index + 10:02d}T08:00:00",
                }
                for index in range(10)
            ],
            *[
                {
                    "id_del_proceso": f"PROC-COBID-{index}",
                    "referencia_del_proceso": f"REF-PROC-COBID-{index}",
                    "id_adjudicacion": f"AWD-COBID-{index}",
                    "urlproceso": f"https://secop.example/PROC-COBID-{index}",
                    "nit_del_proveedor_adjudicado": "907000111-1",
                    "nombre_del_proveedor": "Proveedor Cobid A SAS",
                    "nit_entidad": f"80077788{index % 5}",
                    "entidad": f"Comprador Cobid {index % 5}",
                    "departamento_entidad": "BOGOTA",
                    "ciudad_entidad": "BOGOTA",
                    "modalidad_de_contratacion": "Licitacion publica",
                    "tipo_de_contrato": "Servicios",
                    "fase": "Presentacion de ofertas",
                    "estado_del_procedimiento": "Adjudicado",
                    "estado_resumen": "Adjudicado",
                    "adjudicado": "Si",
                    "valor_total_adjudicacion": "250000000",
                    "precio_base": "250000000",
                    "conteo_de_respuestas_a_ofertas": "4",
                    "respuestas_al_procedimiento": "4",
                    "respuestas_externas": "0",
                    "proveedores_unicos_con": "4",
                    "proveedores_invitados": "8",
                    "proveedores_con_invitacion": "8",
                    "fecha_de_publicacion_del": f"2026-05-{index + 1:02d}T08:00:00",
                }
                for index in range(20)
            ],
        ],
    )
    _write_rows(
        tmp_path,
        "wi7w-2nvm",
        [
            row
            for index in range(20)
            for row in (
                {
                    "fecha_de_registro": f"2026-05-{index + 1:02d}T09:00:00",
                    "referencia_de_la_oferta": f"OFFER-A-{index}",
                    "identificador_de_la_oferta": f"OFFER-ID-A-{index}",
                    "valor_de_la_oferta": "100000000",
                    "entidad_compradora": f"Comprador Cobid {index % 5}",
                    "nit_entidad_compradora": f"80077788{index % 5}",
                    "moneda": "COP",
                    "descripcion_del_procedimiento": "Proceso cobid sintetico",
                    "referencia_del_proceso": f"REF-PROC-COBID-{index}",
                    "id_del_proceso_de_compra": f"PROC-COBID-{index}",
                    "modalidad": "Licitacion publica",
                    "invitacion_directa": "No",
                    "nombre_proveedor": "Proveedor Cobid A SAS",
                    "nit_del_proveedor": "907000111-1",
                    "c_digo_entidad": f"ENT-COBID-{index % 5}",
                    "c_digo_proveedor": "PROV-COBID-A",
                    ":id": f"row-offer-a-{index}",
                },
                {
                    "fecha_de_registro": f"2026-05-{index + 1:02d}T09:05:00",
                    "referencia_de_la_oferta": f"OFFER-B-{index}",
                    "identificador_de_la_oferta": f"OFFER-ID-B-{index}",
                    "valor_de_la_oferta": "110000000",
                    "entidad_compradora": f"Comprador Cobid {index % 5}",
                    "nit_entidad_compradora": f"80077788{index % 5}",
                    "moneda": "COP",
                    "descripcion_del_procedimiento": "Proceso cobid sintetico",
                    "referencia_del_proceso": f"REF-PROC-COBID-{index}",
                    "id_del_proceso_de_compra": f"PROC-COBID-{index}",
                    "modalidad": "Licitacion publica",
                    "invitacion_directa": "No",
                    "nombre_proveedor": "Proveedor Cobid B SAS",
                    "nit_del_proveedor": "907000112-7",
                    "c_digo_entidad": f"ENT-COBID-{index % 5}",
                    "c_digo_proveedor": "PROV-COBID-B",
                    ":id": f"row-offer-b-{index}",
                },
            )
        ],
    )
    _write_rows(
        tmp_path,
        "5u9e-g5w9",
        [
            {
                "document_type": "CC",
                "funcionario_id": "123.456.789",
                "full_name": "Servidora Publica",
                "institution_id": "INST-1",
                "institution_name": "Entidad Uno",
                "institution_department": "BOGOTA",
                "institution_municipality": "BOGOTA",
                "administrative_sector": "Control",
                "job_hierarchy_level": "ASESOR",
                "appointment_type": "Libre nombramiento",
                "current_job_title": "Jefe Oficina Asesora",
                "start_date": "2025-01-15",
            }
        ],
    )
    _write_rows(
        tmp_path,
        "8tz7-h3eu",
        [
            {
                "document_type": "CC",
                "document_id": "123456789",
                "declarant_first_name": "Servidora",
                "declarant_second_name": "",
                "declarant_first_lastname": "Publica",
                "declarant_second_lastname": "",
                "entity_name": "Entidad Uno",
                "publication_date": "2026-01-10",
            }
        ],
    )

    results = build_curated()

    assert {result.table for result in results} == {
        "dim_subject_document",
        "dim_company",
        "dim_buyer",
        "dim_person",
        "fct_procurement_contract_awards",
        "signal_feature_procurement_single_bidder_high_value",
        "signal_feature_procurement_large_modifications",
        "signal_feature_procurement_sanctioned_supplier_awarded",
        "signal_feature_procurement_supplier_concentration_across_entities",
        "signal_feature_procurement_contract_value_outlier_by_category",
        "signal_feature_procurement_repeat_awards_same_supplier",
        "signal_feature_procurement_buyer_supplier_network_density",
        "signal_feature_procurement_cartel_risk_cobidding",
        "signal_feature_procurement_payment_plan_anomalies",
        "signal_feature_procurement_contract_suspensions",
        "signal_feature_procurement_contract_execution_delay",
        "signal_feature_procurement_short_bidding_window",
        "signal_feature_procurement_offers_competition_drop",
        "signal_feature_cuentas_claras_donor_supplier_overlap",
        "signal_feature_procurement_politically_exposed_position_supplier_overlap",
        "signal_feature_procurement_related_companies_shared_officer",
        "signal_feature_procurement_cross_source_identity_inconsistency",
    }
    rows_by_table = {result.table: result.rows for result in results}
    assert rows_by_table["fct_procurement_contract_awards"] == 216
    assert rows_by_table["dim_company"] == 7
    assert rows_by_table["dim_buyer"] == 53
    assert rows_by_table["dim_person"] == 1
    assert rows_by_table["signal_feature_procurement_single_bidder_high_value"] == 1
    assert rows_by_table["signal_feature_procurement_large_modifications"] == 1
    assert rows_by_table["signal_feature_procurement_sanctioned_supplier_awarded"] == 1
    assert rows_by_table["signal_feature_procurement_supplier_concentration_across_entities"] == 1
    assert rows_by_table["signal_feature_procurement_contract_value_outlier_by_category"] == 1
    assert rows_by_table["signal_feature_procurement_repeat_awards_same_supplier"] == 1
    assert rows_by_table["signal_feature_procurement_buyer_supplier_network_density"] == 1
    assert rows_by_table["signal_feature_procurement_cartel_risk_cobidding"] == 2
    assert rows_by_table["signal_feature_procurement_payment_plan_anomalies"] == 1
    assert rows_by_table["signal_feature_procurement_contract_suspensions"] == 1
    assert rows_by_table["signal_feature_procurement_contract_execution_delay"] == 1
    assert rows_by_table["signal_feature_procurement_short_bidding_window"] == 1
    assert rows_by_table["signal_feature_procurement_offers_competition_drop"] == 1
    assert rows_by_table["signal_feature_cuentas_claras_donor_supplier_overlap"] == 1
    assert (
        rows_by_table[
            "signal_feature_procurement_politically_exposed_position_supplier_overlap"
        ]
        == 1
    )
    assert rows_by_table["signal_feature_procurement_related_companies_shared_officer"] == 2
    assert (
        rows_by_table["signal_feature_procurement_cross_source_identity_inconsistency"]
        == 1
    )

    con = duckdb.connect()
    try:
        signal_rows = con.execute(
            "SELECT entity_key, contract_id, paco_record_id, join_rule, risk_signal, "
            "evidence_refs[1] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_sanctioned_supplier_awarded"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        concentration_rows = con.execute(
            "SELECT entity_key, contract_count, distinct_buyer_count, total_contract_value, "
            "evidence_refs[1] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_supplier_concentration_across_entities"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        repeat_rows = con.execute(
            "SELECT entity_key, buyer_document_id, contract_count, total_contract_value, "
            "scope_key, evidence_refs[1] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_repeat_awards_same_supplier"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        outlier_rows = con.execute(
            "SELECT entity_key, scope_key, contract_id, contract_value, "
            "category_contract_count, category_value_rank, evidence_refs[1] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_contract_value_outlier_by_category"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        network_rows = con.execute(
            "SELECT entity_key, scope_key, distinct_buyer_count, contract_count, "
            "repeated_buyer_count, repeated_contract_count, "
            "high_value_repeated_buyer_count, repeated_contract_share, evidence_refs[1] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_buyer_supplier_network_density"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        cobidding_rows = con.execute(
            "SELECT entity_key, counterpart_entity_key, scope_key, "
            "shared_process_count, shared_buyer_count, supplier_process_count, "
            "counterpart_process_count, max_side_share, min_side_share, evidence_refs[1] "
            "FROM read_parquet(?) ORDER BY entity_key",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_cartel_risk_cobidding"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        payment_rows = con.execute(
            "SELECT entity_key, scope_key, anomaly_type, contract_value, "
            "advance_payment_value, advance_payment_share, paid_value, "
            "invoiced_value, evidence_refs[1] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_payment_plan_anomalies"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        suspension_rows = con.execute(
            "SELECT entity_key, scope_key, contract_value, suspension_event_count, "
            "resumption_event_count, distinct_suspension_dates, suspension_span_days, "
            "evidence_refs[1], evidence_refs[2], evidence_refs[3] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_contract_suspensions"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        single_bidder_rows = con.execute(
            "SELECT entity_key, contract_id, scope_key, severity, contract_value, evidence_refs[1] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_single_bidder_high_value"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        large_modification_rows = con.execute(
            "SELECT entity_key, scope_key, contract_value, total_modification_value, "
            "round(modification_value_share, 2), evidence_refs[1], evidence_refs[2] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_large_modifications"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        execution_delay_rows = con.execute(
            "SELECT entity_key, scope_key, severity, contract_value, delayed_item_count, "
            "max_delay_days, evidence_refs[1], evidence_refs[2] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_contract_execution_delay"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        short_window_rows = con.execute(
            "SELECT entity_key, scope_key, severity, estimated_value, "
            "response_count, open_window_hours, evidence_refs[1] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_short_bidding_window"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        competition_drop_rows = con.execute(
            "SELECT entity_key, scope_key, recent_process_count, prior_process_count, "
            "recent_low_response_share, prior_low_response_share, evidence_refs[1] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_offers_competition_drop"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        cuentas_claras_rows = con.execute(
            "SELECT entity_key, scope_key, donor_name, candidate_name, "
            "total_income_amount, post_2019_contract_count, post_2019_contract_value, "
            "evidence_refs[1], evidence_refs[2] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_cuentas_claras_donor_supplier_overlap"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        exposed_position_rows = con.execute(
            "SELECT entity_key, scope_key, representative_document_key, "
            "institution_id, contract_count, total_contract_value, "
            "evidence_refs[1], evidence_refs[2], evidence_refs[3] "
            "FROM read_parquet(?)",
            [
                str(tmp_path / "curated" / _exposed_position_feature_table() / "*.parquet")
            ],
        ).fetchall()
        shared_officer_rows = con.execute(
            "SELECT entity_key, scope_key, representative_document_key, "
            "linked_company_count, contract_count, total_contract_value, "
            "evidence_refs[1], evidence_refs[2], evidence_refs[3] "
            "FROM read_parquet(?) ORDER BY entity_key",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_related_companies_shared_officer"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        identity_inconsistency_rows = con.execute(
            "SELECT entity_key, scope_key, company_name, supplier_name, "
            "name_mismatch_flag, representative_document_mismatch_flag, "
            "mismatch_dimension_count, evidence_refs[1], evidence_refs[2] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_cross_source_identity_inconsistency"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        company_rows = con.execute(
            "SELECT nit_canonical, sources FROM read_parquet(?) ORDER BY nit_canonical",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=dim_company"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        person_rows = con.execute(
            "SELECT cedula_canonical, nit_canonical, sources FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=dim_person"
                    / "*.parquet"
                )
            ],
        ).fetchall()
    finally:
        con.close()
    assert signal_rows == [
        (
            "900123456",
            "C-1",
            "paco-1",
            "nit_base_to_subject",
            1.0,
            "https://secop.example/C-1",
        )
    ]
    assert concentration_rows == [
        ("900765432", 50, 50, 5_000_000_000.0, "https://secop.example/CX-0")
    ]
    assert repeat_rows == [
        (
            "901000111",
            "800999888",
            10,
            2_000_000_000.0,
            "buyer:800999888",
            "https://secop.example/CR-0",
        )
    ]
    assert outlier_rows == [
        (
            "CO-OUTLIER-1",
            "CO-OUTLIER-1",
            "CO-OUTLIER-1",
            5_000_000_000.0,
            101,
            1,
            "https://secop.example/CO-OUTLIER-1",
        )
    ]
    assert network_rows == [
        (
            "906000111",
            "supplier_network:906000111",
            10,
            50,
            10,
            50,
            10,
            1.0,
            "https://secop.example/CN-0-0",
        )
    ]
    assert cobidding_rows == [
        (
            "907000111",
            "907000112",
            "cobid_pair:907000111:907000112",
            20,
            5,
            20,
            20,
            1.0,
            1.0,
            "https://secop.example/PROC-COBID-19",
        ),
        (
            "907000112",
            "907000111",
            "cobid_pair:907000111:907000112",
            20,
            5,
            20,
            20,
            1.0,
            1.0,
            "https://secop.example/PROC-COBID-19",
        ),
    ]
    assert payment_rows == [
        (
            "907000113",
            "CPAY-1",
            "high_advance_payment_share",
            1_000_000_000.0,
            600_000_000.0,
            0.6,
            0.0,
            0.0,
            "https://secop.example/CPAY-1",
        )
    ]
    assert suspension_rows == [
        (
            "908000113",
            "CSUSP-1",
            250_000_000.0,
            2,
            1,
            2,
            31,
            "https://secop.example/CSUSP-1",
            "secop_contract_suspensions:CSUSP-1:2026-06-11",
            "secop_contract_suspensions:CSUSP-1:2026-05-11",
        )
    ]
    assert single_bidder_rows == [
        (
            "902000111",
            "CV-1",
            "PV-1",
            "medium",
            2_000_000_000.0,
            "https://secop.example/CV-1",
        )
    ]
    assert large_modification_rows == [
        (
            "900123456",
            "C-1",
            125_000_000.0,
            75_000_000.0,
            0.6,
            "https://secop.example/C-1",
            "secop_contract_modifications:MOD-C-1",
        )
    ]
    assert execution_delay_rows == [
        (
            "900123456",
            "C-1",
            "low",
            125_000_000.0,
            1,
            46,
            "https://secop.example/C-1",
            "secop_contract_execution:C-1:ITEM-C-1",
        )
    ]
    assert short_window_rows == [
        (
            "903000111",
            "PROC-SHORT-1",
            "low",
            300_000_000.0,
            1,
            24,
            "https://secop.example/PROC-SHORT-1",
        )
    ]
    assert competition_drop_rows == [
        (
            "800555666",
            "buyer:800555666",
            20,
            10,
            1.0,
            0.0,
            "https://secop.example/PROC-DROP-R-19",
        )
    ]
    assert cuentas_claras_rows == [
        (
            "900123456",
            "election:2019:900123456:700111222",
            "Proveedor Sancionado SAS",
            "Candidata Uno",
            5_000_000.0,
            1,
            125_000_000.0,
            "cuentas_claras_income_2019:VOUCHER-1",
            "https://secop.example/C-1",
        )
    ]
    assert exposed_position_rows == [
        (
            "902000111",
            "sensitive_position:123456789:INST-1:902000111",
            "123456789",
            "INST-1",
            1,
            2_000_000_000.0,
            "company_registry_c82u:row-c82u-1",
            "sigep_sensitive_positions:123456789",
            "https://secop.example/CV-1",
        )
    ]
    assert shared_officer_rows == [
        (
            "900765432",
            "officer_cluster:222222222",
            "222222222",
            2,
            50,
            5_000_000_000.0,
            "company_registry_c82u:row-c82u-shared-1",
            "company_registry_c82u:row-c82u-shared-2",
            "https://secop.example/CX-0",
        ),
        (
            "901000111",
            "officer_cluster:222222222",
            "222222222",
            2,
            10,
            2_000_000_000.0,
            "company_registry_c82u:row-c82u-shared-1",
            "company_registry_c82u:row-c82u-shared-2",
            "https://secop.example/CR-0",
        ),
    ]
    assert identity_inconsistency_rows == [
        (
            "902000111",
            "alias_cluster:902000111",
            "Proveedor Valor Alto SAS",
            "Comercial Valor Alto Diferente SAS",
            True,
            True,
            2,
            "company_registry_c82u:row-c82u-1",
            "secop_suppliers:SUP-902",
        )
    ]
    assert company_rows == [
        ("9001234568", ["paco_sanctions", "secop_ii_contracts"]),
        ("9007654326", ["secop_ii_contracts"]),
        ("9010001118", ["secop_ii_contracts"]),
        ("9020001111", ["secop_ii_contracts"]),
        ("9060001116", ["secop_ii_contracts"]),
        ("9070001134", ["secop_ii_contracts"]),
        ("9080001138", ["secop_ii_contracts"]),
    ]
    assert person_rows == [("123456789", None, ["5u9e-g5w9", "8tz7-h3eu"])]


def test_build_curated_errors_when_required_sources_are_missing(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))

    with pytest.raises(CuratedBuildError, match="missing required lake source"):
        build_curated()


def test_build_dim_person_without_procurement_sources(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("COACC_LAKE_ROOT", str(tmp_path))
    _write_rows(
        tmp_path,
        "5u9e-g5w9",
        [
            {
                "document_type": "CC",
                "funcionario_id": "987654321",
                "full_name": "Funcionario Uno",
                "institution_id": "INST-2",
                "institution_name": "Entidad Dos",
                "start_date": "2025-02-01",
            }
        ],
    )
    _write_rows(
        tmp_path,
        "8tz7-h3eu",
        [
            {
                "document_type": "CC",
                "document_id": "987654321",
                "declarant_first_name": "Funcionario",
                "declarant_second_name": "",
                "declarant_first_lastname": "Uno",
                "declarant_second_lastname": "",
                "entity_name": "Entidad Dos",
                "publication_date": "2026-02-01",
            }
        ],
    )

    results = build_curated(["dim_person"])

    assert [(result.table, result.rows) for result in results] == [("dim_person", 1)]
