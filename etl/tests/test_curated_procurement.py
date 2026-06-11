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
                "supervisor_name": "Persona Conflicto",
                "supervisor_doc_type": "CC",
                "supervisor_doc_number": "1001234567",
                "spending_orderer_name": "",
                "spending_orderer_doc_type": "",
                "spending_orderer_doc_number": "",
                "payment_orderer_name": "",
                "payment_orderer_doc_type": "",
                "payment_orderer_doc_number": "",
                "entity_nit": "800111222",
                "entity_name": "Comprador Uno",
                "department": "NARINO",
                "city": "TUMACO",
                "sector": "Salud",
                "procurement_modality": "Contratacion directa",
                "contract_type": "Prestacion de servicios",
                "contract_status": "Activo",
                "contract_object": "",
                "process_description": "",
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
                "signing_date": "2021-05-01",
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
                "contract_id": "SECOP-SAN-1",
                "contract_reference": "REF-SECOP-SAN-1",
                "procurement_process": "P-SECOP-SAN-1",
                "process_url": "https://secop.example/SECOP-SAN-1",
                "supplier_document": "905123999-3",
                "supplier_doc_type": "NIT",
                "awarded_supplier": "Proveedor Secop Sancion SAS",
                "entity_nit": "800111222",
                "entity_name": "Comprador Uno",
                "department": "NARINO",
                "city": "TUMACO",
                "sector": "Sancion QA",
                "procurement_modality": "Licitacion",
                "contract_type": "Servicios",
                "contract_value": "200000000",
                "signing_date": "2021-01-15",
                "contract_start_date": "2021-01-16",
                "contract_end_date": "2021-12-31",
                "last_update": "2021-01-17T00:00:00",
            },
            {
                "contract_id": "SECOP-SAN-LATER",
                "contract_reference": "REF-SECOP-SAN-LATER",
                "procurement_process": "P-SECOP-SAN-LATER",
                "process_url": "https://secop.example/SECOP-SAN-LATER",
                "supplier_document": "905123999-3",
                "supplier_doc_type": "NIT",
                "awarded_supplier": "Proveedor Secop Sancion SAS",
                "entity_nit": "800111222",
                "entity_name": "Comprador Uno",
                "department": "NARINO",
                "city": "TUMACO",
                "sector": "Sancion QA",
                "procurement_modality": "Contratacion directa",
                "contract_type": "Servicios",
                "contract_value": "150000000",
                "signing_date": "2021-07-01",
                "contract_start_date": "2021-07-02",
                "contract_end_date": "2021-12-31",
                "last_update": "2021-07-03T00:00:00",
            },
            {
                "contract_id": "CPER-1",
                "contract_reference": "REF-PER-1",
                "procurement_process": "PPER-1",
                "process_url": "https://secop.example/CPER-1",
                "supplier_document": "1001234567",
                "supplier_doc_type": "CC",
                "awarded_supplier": "Persona Conflicto",
                "entity_nit": "800111222",
                "entity_name": "Comprador Uno",
                "department": "NARINO",
                "city": "TUMACO",
                "sector": "Salud",
                "procurement_modality": "Contratacion directa",
                "contract_type": "Servicios",
                "contract_value": "800000000",
                "signing_date": "2026-05-02",
                "contract_start_date": "2026-05-03",
                "contract_end_date": "2026-12-31",
                "last_update": "2026-05-04T00:00:00",
            },
            {
                "contract_id": "CPER-2",
                "contract_reference": "REF-PER-2",
                "procurement_process": "PPER-2",
                "process_url": "https://secop.example/CPER-2",
                "supplier_document": "1001234567",
                "supplier_doc_type": "CC",
                "awarded_supplier": "Persona Conflicto",
                "entity_nit": "800111333",
                "entity_name": "Comprador Dos",
                "department": "CAUCA",
                "city": "POPAYAN",
                "sector": "Educacion",
                "procurement_modality": "Licitacion",
                "contract_type": "Suministro",
                "contract_value": "750000000",
                "signing_date": "2026-05-03",
                "contract_start_date": "2026-05-04",
                "contract_end_date": "2026-12-31",
                "last_update": "2026-05-05T00:00:00",
            },
            {
                "contract_id": "CPER-3",
                "contract_reference": "REF-PER-3",
                "procurement_process": "PPER-3",
                "process_url": "https://secop.example/CPER-3",
                "supplier_document": "1001234567",
                "supplier_doc_type": "CC",
                "awarded_supplier": "Persona Conflicto",
                "entity_nit": "800111333",
                "entity_name": "Comprador Dos",
                "department": "CAUCA",
                "city": "POPAYAN",
                "sector": "Educacion",
                "procurement_modality": "Licitacion",
                "contract_type": "Suministro",
                "contract_value": "500000000",
                "signing_date": "2026-05-04",
                "contract_start_date": "2026-05-05",
                "contract_end_date": "2026-12-31",
                "last_update": "2026-05-06T00:00:00",
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
                "contract_object": (
                    "Prestacion de servicios de salud hospitalarios y apoyo al "
                    "programa de alimentacion escolar PAE"
                ),
                "process_description": (
                    "Servicios medicos y de alimentacion escolar con seguimiento "
                    "de pagos"
                ),
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
                "contract_end_date": "2026-04-30",
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
            {
                "contract_id": "CLADDER-1",
                "contract_reference": "REF-LADDER-1",
                "procurement_process": "PLADDER-1",
                "process_url": "https://secop.example/CLADDER-1",
                "supplier_document": "908000113-8",
                "supplier_doc_type": "NIT",
                "awarded_supplier": "Proveedor Suspendido SAS",
                "entity_nit": "800111222",
                "entity_name": "Comprador Uno",
                "department": "BOGOTA",
                "city": "BOGOTA",
                "sector": "Modificacion QA",
                "procurement_modality": "Contratación directa",
                "contract_type": "Obra",
                "contract_status": "Activo",
                "enables_advance_payment": "No",
                "liquidation": "No",
                "contract_value": "600000000",
                "advance_payment_value": "0",
                "invoiced_value": "0",
                "pending_payment_value": "600000000",
                "paid_value": "0",
                "amortized_value": "0",
                "pending_value": "600000000",
                "pending_execution_value": "600000000",
                "added_days": "210",
                "signing_date": "2026-04-05",
                "contract_start_date": "2026-04-06",
                "contract_end_date": "2026-12-31",
                "last_update": "2026-06-12T00:00:00",
            },
            {
                "contract_id": "CREL-1",
                "contract_reference": "REF-RELATED-1",
                "procurement_process": "PREL-1",
                "process_url": "https://secop.example/CREL-1",
                "supplier_document": "900765432-6",
                "supplier_doc_type": "NIT",
                "awarded_supplier": "Proveedor Concentrado SAS",
                "entity_nit": "800999888",
                "entity_name": "Comprador Recurrente",
                "department": "ANTIOQUIA",
                "city": "MEDELLIN",
                "sector": "Competencia QA",
                "procurement_modality": "Licitacion publica",
                "contract_type": "Servicios",
                "contract_status": "Activo",
                "enables_advance_payment": "No",
                "liquidation": "No",
                "contract_value": "800000000",
                "advance_payment_value": "0",
                "invoiced_value": "0",
                "pending_payment_value": "800000000",
                "paid_value": "0",
                "amortized_value": "0",
                "pending_value": "800000000",
                "pending_execution_value": "800000000",
                "added_days": "0",
                "signing_date": "2026-05-15",
                "contract_start_date": "2026-05-16",
                "contract_end_date": "2026-12-31",
                "last_update": "2026-05-16T00:00:00",
            },
            {
                "contract_id": "CSHARED-1",
                "contract_reference": "REF-SHARED-1",
                "procurement_process": "PSHARED-1",
                "process_url": "https://secop.example/CSHARED-1",
                "supplier_document": "910111222-6",
                "supplier_doc_type": "NIT",
                "awarded_supplier": "Proveedor Mismo Representante Uno SAS",
                "entity_nit": "800999888",
                "entity_name": "Comprador Recurrente",
                "department": "ANTIOQUIA",
                "city": "MEDELLIN",
                "sector": "Competencia QA",
                "procurement_modality": "Contratacion directa",
                "contract_type": "Servicios",
                "contract_status": "Activo",
                "enables_advance_payment": "No",
                "liquidation": "No",
                "contract_value": "900000000",
                "advance_payment_value": "0",
                "invoiced_value": "0",
                "pending_payment_value": "900000000",
                "paid_value": "0",
                "amortized_value": "0",
                "pending_value": "900000000",
                "pending_execution_value": "900000000",
                "added_days": "0",
                "signing_date": "2026-06-01",
                "contract_start_date": "2026-06-02",
                "contract_end_date": "2026-12-31",
                "last_update": "2026-06-02T00:00:00",
            },
            {
                "contract_id": "CSHARED-2",
                "contract_reference": "REF-SHARED-2",
                "procurement_process": "PSHARED-2",
                "process_url": "https://secop.example/CSHARED-2",
                "supplier_document": "910111222-6",
                "supplier_doc_type": "NIT",
                "awarded_supplier": "Proveedor Mismo Representante Uno SAS",
                "entity_nit": "800999888",
                "entity_name": "Comprador Recurrente",
                "department": "ANTIOQUIA",
                "city": "MEDELLIN",
                "sector": "Competencia QA",
                "procurement_modality": "Contratacion directa",
                "contract_type": "Servicios",
                "contract_status": "Activo",
                "enables_advance_payment": "No",
                "liquidation": "No",
                "contract_value": "900000000",
                "advance_payment_value": "0",
                "invoiced_value": "0",
                "pending_payment_value": "900000000",
                "paid_value": "0",
                "amortized_value": "0",
                "pending_value": "900000000",
                "pending_execution_value": "900000000",
                "added_days": "0",
                "signing_date": "2026-06-02",
                "contract_start_date": "2026-06-03",
                "contract_end_date": "2026-12-31",
                "last_update": "2026-06-03T00:00:00",
            },
            {
                "contract_id": "CSHARED-3",
                "contract_reference": "REF-SHARED-3",
                "procurement_process": "PSHARED-3",
                "process_url": "https://secop.example/CSHARED-3",
                "supplier_document": "910111222-6",
                "supplier_doc_type": "NIT",
                "awarded_supplier": "Proveedor Mismo Representante Uno SAS",
                "entity_nit": "800999888",
                "entity_name": "Comprador Recurrente",
                "department": "ANTIOQUIA",
                "city": "MEDELLIN",
                "sector": "Competencia QA",
                "procurement_modality": "Contratacion directa",
                "contract_type": "Servicios",
                "contract_status": "Activo",
                "enables_advance_payment": "No",
                "liquidation": "No",
                "contract_value": "900000000",
                "advance_payment_value": "0",
                "invoiced_value": "0",
                "pending_payment_value": "900000000",
                "paid_value": "0",
                "amortized_value": "0",
                "pending_value": "900000000",
                "pending_execution_value": "900000000",
                "added_days": "0",
                "signing_date": "2026-06-03",
                "contract_start_date": "2026-06-04",
                "contract_end_date": "2026-12-31",
                "last_update": "2026-06-04T00:00:00",
            },
            {
                "contract_id": "CSHARED-4",
                "contract_reference": "REF-SHARED-4",
                "procurement_process": "PSHARED-4",
                "process_url": "https://secop.example/CSHARED-4",
                "supplier_document": "910111222-6",
                "supplier_doc_type": "NIT",
                "awarded_supplier": "Proveedor Mismo Representante Uno SAS",
                "entity_nit": "800999888",
                "entity_name": "Comprador Recurrente",
                "department": "ANTIOQUIA",
                "city": "MEDELLIN",
                "sector": "Competencia QA",
                "procurement_modality": "Contratacion directa",
                "contract_type": "Servicios",
                "contract_status": "Activo",
                "enables_advance_payment": "No",
                "liquidation": "No",
                "contract_value": "900000000",
                "advance_payment_value": "0",
                "invoiced_value": "0",
                "pending_payment_value": "900000000",
                "paid_value": "0",
                "amortized_value": "0",
                "pending_value": "900000000",
                "pending_execution_value": "900000000",
                "added_days": "0",
                "signing_date": "2026-06-04",
                "contract_start_date": "2026-06-05",
                "contract_end_date": "2026-12-31",
                "last_update": "2026-06-05T00:00:00",
            },
            {
                "contract_id": "CSHARED-5",
                "contract_reference": "REF-SHARED-5",
                "procurement_process": "PSHARED-5",
                "process_url": "https://secop.example/CSHARED-5",
                "supplier_document": "910111333-5",
                "supplier_doc_type": "NIT",
                "awarded_supplier": "Proveedor Mismo Representante Dos SAS",
                "entity_nit": "800999888",
                "entity_name": "Comprador Recurrente",
                "department": "ANTIOQUIA",
                "city": "MEDELLIN",
                "sector": "Competencia QA",
                "procurement_modality": "Contratacion directa",
                "contract_type": "Servicios",
                "contract_status": "Activo",
                "enables_advance_payment": "No",
                "liquidation": "No",
                "contract_value": "900000000",
                "advance_payment_value": "0",
                "invoiced_value": "0",
                "pending_payment_value": "900000000",
                "paid_value": "0",
                "amortized_value": "0",
                "pending_value": "900000000",
                "pending_execution_value": "900000000",
                "added_days": "0",
                "signing_date": "2026-06-05",
                "contract_start_date": "2026-06-06",
                "contract_end_date": "2026-12-31",
                "last_update": "2026-06-06T00:00:00",
            },
            {
                "contract_id": "CSHARED-6",
                "contract_reference": "REF-SHARED-6",
                "procurement_process": "PSHARED-6",
                "process_url": "https://secop.example/CSHARED-6",
                "supplier_document": "910111333-5",
                "supplier_doc_type": "NIT",
                "awarded_supplier": "Proveedor Mismo Representante Dos SAS",
                "entity_nit": "800999888",
                "entity_name": "Comprador Recurrente",
                "department": "ANTIOQUIA",
                "city": "MEDELLIN",
                "sector": "Competencia QA",
                "procurement_modality": "Contratacion directa",
                "contract_type": "Servicios",
                "contract_status": "Activo",
                "enables_advance_payment": "No",
                "liquidation": "No",
                "contract_value": "900000000",
                "advance_payment_value": "0",
                "invoiced_value": "0",
                "pending_payment_value": "900000000",
                "paid_value": "0",
                "amortized_value": "0",
                "pending_value": "900000000",
                "pending_execution_value": "900000000",
                "added_days": "0",
                "signing_date": "2026-06-06",
                "contract_start_date": "2026-06-07",
                "contract_end_date": "2026-12-31",
                "last_update": "2026-06-07T00:00:00",
            },
            {
                "contract_id": "CREUSE-1",
                "contract_reference": "REF-REUSE-1",
                "procurement_process": "PREUSE-1",
                "process_url": "https://secop.example/CREUSE-1",
                "supplier_document": "909000111-5",
                "supplier_doc_type": "NIT",
                "awarded_supplier": "Proveedor Poliza Reusada Uno SAS",
                "entity_nit": "800222333",
                "entity_name": "Comprador Poliza Uno",
                "department": "BOLIVAR",
                "city": "CARTAGENA",
                "sector": "Garantias QA",
                "procurement_modality": "Licitacion publica",
                "contract_type": "Servicios",
                "contract_status": "Activo",
                "enables_advance_payment": "No",
                "liquidation": "No",
                "contract_value": "600000000",
                "advance_payment_value": "0",
                "invoiced_value": "0",
                "pending_payment_value": "600000000",
                "paid_value": "0",
                "amortized_value": "0",
                "pending_value": "600000000",
                "pending_execution_value": "600000000",
                "added_days": "0",
                "signing_date": "2026-05-17",
                "contract_start_date": "2026-05-18",
                "contract_end_date": "2026-12-31",
                "last_update": "2026-05-18T00:00:00",
            },
            {
                "contract_id": "CREUSE-2",
                "contract_reference": "REF-REUSE-2",
                "procurement_process": "PREUSE-2",
                "process_url": "https://secop.example/CREUSE-2",
                "supplier_document": "909000222-4",
                "supplier_doc_type": "NIT",
                "awarded_supplier": "Proveedor Poliza Reusada Dos SAS",
                "entity_nit": "800222444",
                "entity_name": "Comprador Poliza Dos",
                "department": "CORDOBA",
                "city": "MONTERIA",
                "sector": "Garantias QA",
                "procurement_modality": "Licitacion publica",
                "contract_type": "Servicios",
                "contract_status": "Activo",
                "enables_advance_payment": "No",
                "liquidation": "No",
                "contract_value": "700000000",
                "advance_payment_value": "0",
                "invoiced_value": "0",
                "pending_payment_value": "700000000",
                "paid_value": "0",
                "amortized_value": "0",
                "pending_value": "700000000",
                "pending_execution_value": "700000000",
                "added_days": "0",
                "signing_date": "2026-05-19",
                "contract_start_date": "2026-05-20",
                "contract_end_date": "2026-12-31",
                "last_update": "2026-05-20T00:00:00",
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
            {
                "contract_id": "BPIN-C-1",
                "contract_reference": "REF-BPIN-1",
                "procurement_process": "PROC-BPIN-1",
                "process_url": "https://secop.example/BPIN-C-1",
                "supplier_document": "903333111-4",
                "supplier_doc_type": "NIT",
                "awarded_supplier": "Proveedor BPIN Uno SAS",
                "entity_nit": "800777888",
                "entity_name": "Comprador BPIN",
                "department": "BOGOTA",
                "city": "BOGOTA",
                "sector": "BPIN QA",
                "procurement_modality": "Licitacion publica",
                "contract_type": "Obra",
                "contract_value": "70000000000",
                "signing_date": "2026-05-04",
                "contract_start_date": "2026-05-05",
                "contract_end_date": "2026-12-31",
                "last_update": "2026-05-06T00:00:00",
            },
            {
                "contract_id": "BPIN-C-2",
                "contract_reference": "REF-BPIN-2",
                "procurement_process": "PROC-BPIN-2",
                "process_url": "https://secop.example/BPIN-C-2",
                "supplier_document": "903333222-3",
                "supplier_doc_type": "NIT",
                "awarded_supplier": "Proveedor BPIN Dos SAS",
                "entity_nit": "800777888",
                "entity_name": "Comprador BPIN",
                "department": "BOGOTA",
                "city": "BOGOTA",
                "sector": "BPIN QA",
                "procurement_modality": "Licitacion publica",
                "contract_type": "Obra",
                "contract_value": "50000000000",
                "signing_date": "2026-05-03",
                "contract_start_date": "2026-05-04",
                "contract_end_date": "2026-12-31",
                "last_update": "2026-05-05T00:00:00",
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
        "8qxx-ubmq",
        [
            {
                "nit": "900123456",
                "entity_name": "Proveedor Sancionado SAS",
                "process": "Contrato de obra con hallazgo fiscal",
                "audited_year": "2020",
                "report_date": "2020-01-15T00:00:00.000",
                "facts": "Hallazgo fiscal de prueba con pagos sin soporte completo.",
                "amount": "50000000",
                "received_date": "2020-02-01T00:00:00.000",
                "radicado": "HF-1",
                "status": "EN ESTUDIO EN DEPENDENCIA COMPETENTE",
                "procedure_date": "2020-02-10T00:00:00.000",
                "observations": "NO REGISTRA",
            }
        ],
    )
    _write_rows(
        tmp_path,
        "jr8e-e8tu",
        [
            {
                "entity_name": "Proveedor Sancionado SAS",
                "document_type": "NIT",
                "nit": "900123456",
                "sanction_type": "Fallo con Responsabilidad Fiscal",
                "topic": "Responsabilidad Fiscal",
                "n_mero_de_resoluci_n_de_la": "RF-2020-1",
                "resolution_date": "2020-03-01T00:00:00.000",
                "amount": "$ 150,000,000.00",
                "appeal_resolution_number": "AR-1",
                "appeal_resolution_date": "2020-04-01T00:00:00.000",
                "appeal_info": "Apelacion",
                "description": "Fallo fiscal de prueba.",
                "source_system": "SIREF",
                "decision_finality_date": "2020-05-01T00:00:00.000",
            }
        ],
    )
    _write_rows(
        tmp_path,
        "iaeu-rcn6",
        [
            {
                "siri_number": "SIRI-1",
                "ineligibility_type": "DISCIPLINARIO",
                "person_quality": "SERVIDOR PUBLICO",
                "identification_type_code": "1",
                "identification_type_name": "CEDULA DE CIUDADANIA",
                "document_id": "1001234567",
                "first_last_name": "CONFLICTO",
                "second_last_name": "",
                "first_name": "PERSONA",
                "second_name": "",
                "role_or_position": "Supervisor",
                "facts_department": "NARINO",
                "facts_municipality": "TUMACO",
                "sanctions": "INHABILIDAD GENERAL",
                "duration_years": "10",
                "duration_months": "0",
                "duration_days": "0",
                "decision_instance": "PRIMERA",
                "authority": "PROCURADURIA TEST",
                "legal_effects_date": "01/01/2021",
                "process_number": "PROC-SIRI-1",
                "sanctioned_entity": "ENTIDAD TEST",
                "sanctioned_entity_department": "NARINO",
                "sanctioned_entity_municipality": "TUMACO",
            }
        ],
    )
    _write_rows(
        tmp_path,
        "u99c-7mfm",
        [
            {
                "id_contrato": "CPAY-1",
                "tipo": "Suspension",
                "fecha_de_creacion": "2026-05-14T00:00:00",
                "fecha_de_aprobacion": "2026-05-15T00:00:00",
                "proposito_de_la_modificacion": "Suspension por revision de avance",
                "fecha_de_inicio_del_contrato": "2026-05-15",
                "fecha_de_fin_del_contrato": "2026-06-15",
            },
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
        "gjp9-cutm",
        [
            {
                "contract_id": "CPAY-1",
                "insurer": "SEGUROS TEST",
                "policy_number": "POL-ACCEPTED-LATE",
                "insured": "Proveedor Pago Anomalo SAS",
                "beneficiary": "Comprador Uno",
                "policy_created_at": "2026-05-10 08:00:00.0000000 -05:00",
                "policy_sent_at": "2026-05-10 09:00:00.0000000 -05:00",
                "policy_end_at": "2026-12-31 23:59:00.0000000 -05:00",
                "policy_side": "PolizaProveedor",
                "status": "Aceptada",
                "policy_type": "Contrato de Seguro",
                "policy_subtype": "No Definido",
                "policy_value": "300000000",
                "created_at": "2026-05-10 08:00:00.0000000 -05:00",
            },
            {
                "contract_id": "CPAY-1",
                "insurer": "SEGUROS TEST",
                "policy_number": "POL-REJECTED",
                "insured": "Proveedor Pago Anomalo SAS",
                "beneficiary": "Comprador Uno",
                "policy_created_at": "2026-05-11 08:00:00.0000000 -05:00",
                "policy_sent_at": "2026-05-11 09:00:00.0000000 -05:00",
                "policy_end_at": "2026-12-31 23:59:00.0000000 -05:00",
                "policy_side": "PolizaProveedor",
                "status": "Rechazada",
                "policy_type": "Contrato de Seguro",
                "policy_subtype": "No Definido",
                "policy_value": "100000000",
                "created_at": "2026-05-11 08:00:00.0000000 -05:00",
            },
            {
                "contract_id": "CREUSE-1",
                "insurer": "SEGUROS MUNDIAL",
                "policy_number": "100777888",
                "insured": "Proveedor Poliza Reusada Uno SAS",
                "beneficiary": "Comprador Poliza Uno",
                "policy_created_at": "2026-05-17 08:00:00.0000000 -05:00",
                "policy_sent_at": "2026-05-17 09:00:00.0000000 -05:00",
                "policy_end_at": "2026-12-31 23:59:00.0000000 -05:00",
                "policy_side": "PolizaProveedor",
                "status": "Aceptada",
                "policy_type": "Contrato de Seguro",
                "policy_subtype": "Cumplimiento",
                "policy_value": "180000000",
                "created_at": "2026-05-17 08:00:00.0000000 -05:00",
            },
            {
                "contract_id": "CREUSE-2",
                "insurer": "SEGUROS MUNDIAL",
                "policy_number": "100777888",
                "insured": "Proveedor Poliza Reusada Dos SAS",
                "beneficiary": "Comprador Poliza Dos",
                "policy_created_at": "2026-05-19 08:00:00.0000000 -05:00",
                "policy_sent_at": "2026-05-19 09:00:00.0000000 -05:00",
                "policy_end_at": "2026-12-31 23:59:00.0000000 -05:00",
                "policy_side": "PolizaProveedor",
                "status": "Expirada",
                "policy_type": "Contrato de Seguro",
                "policy_subtype": "Cumplimiento",
                "policy_value": "210000000",
                "created_at": "2026-05-19 08:00:00.0000000 -05:00",
            },
        ],
    )
    _write_rows(
        tmp_path,
        "a86w-fh92",
        [
            {
                "id_contrato": "CPAY-1",
                "referencia_contrato": "REF-PAY-1",
                "entidad": "Comprador Uno",
                "nit": "800111222",
                "departamento": "BOGOTA",
                "ciudad": "BOGOTA",
                "proveedor": "Proveedor Pago Anomalo SAS",
                "nit_proveedor": "907000113",
                "id_proceso": "PPAY-1",
                "id_portafolio": "PORT-PAY-1",
                "referencia_del_proceso": "REF-PAY-1",
                "estado_del_contrato": "Seleccionado",
                "ultima_consulta_siif": "No definido",
                "estado_siif": "No iniciado",
                "id_siif": "SIIF-CDP-CPAY-1",
                "destino_del_gasto": "Inversion",
                "saldo_cdp": "0",
                "saldo_vigencias_futuras": "0",
                "registrado_en_siif": False,
                "c_digo": "CDP-CPAY-1",
                "tipo_vigencias_futuras": "CDP",
                "saldo_total_a_comprometer": "0",
                "valor_utilizado": "0",
                "codigo_unidad_ejecutora": "UE-1",
                "pilar_acuerdo_paz": "No disponible",
                "acuerdo_marco": "No",
                "fuente_de_los_recursos": "Presupuesto territorial nacional",
                "presupuesto_general_estado": "0",
                "sistema_nacional_participaciones": "0",
                "sistema_general_de_regal_as": "0",
                "recursos_propios_agri": "0",
                "recursos_de_credito": "0",
                "recursos_propios": "0",
                "entidad_bpin": "No disponible",
                "bpin_codigo": "No disponible",
                "a_o_bpin": "No disponible",
                "bpin_validacion": "No definido",
                "c_digo_cdp": "CDP-CPAY-1",
                "pci_unidad_subejecutora": "UE-1",
            }
        ],
    )
    _write_rows(
        tmp_path,
        "skc9-met7",
        [
            {
                "identificador_nico": "COMP-CPAY-1",
                "id_contrato": "CPAY-1",
                "referencia_contrato": "REF-PAY-1",
                "balance_compromiso": "0",
                "balance_vigencia_futura": "0",
                "estado_integraci_n": "Fallido",
                "identificador_item": "COMPITEM-CPAY-1",
                "tipo_de_compromiso": "0",
                "c_digo_item": "No Definido",
                "valor_item": "0",
                "estado_integraci_n_item": "No Iniciado",
                "valor_a_liberar": "0",
            }
        ],
    )
    _write_rows(
        tmp_path,
        "cwhv-7fnp",
        [
            {
                "identificador_unico": "RUBRO-CPAY-1",
                "identificador_item_compromiso": "COMPITEM-CPAY-1",
                "identificador_compromiso": "COMP-CPAY-1",
                "id_contrato": "CPAY-1",
                "referencia_contrato": "REF-PAY-1",
                "codigo": "No Definido",
                "nombre": "NO DEFINIDO",
                "valor_actual": "0",
                "anno": "2026",
            }
        ],
    )
    _write_rows(
        tmp_path,
        "ibyt-yi2f",
        [
            {
                "id_contrato": "CPAY-1",
                "id_pago": "INV-CPAY-1",
                "numero_de_factura": "FAC-CPAY-1",
                "fecha_factura": "2026-05-12",
                "fecha_de_entrega": "2026-05-12",
                "fecha_estimada_de_pago": "2026-05-20",
                "valor_total": "1400000000",
                "valor_a_pagar": "1400000000",
                "valor_neto": "1400000000",
                "estado": "Aprobado",
                "pago_confirmado": True,
                "codigo_entidad": "800111222",
            }
        ],
    )
    _write_rows(
        tmp_path,
        "uymx-8p3j",
        [
            {
                "id_del_contrato": "CPAY-1",
                "id_de_pago": "PAY-CPAY-1",
                "numero_de_factura": "FAC-CPAY-1",
                "fecha_de_emision": "2026-05-12",
                "fecha_de_recepcion": "2026-05-13",
                "fecha_estimada_de_pago": "2026-05-20",
                "fecha_real_de_pago": "2026-05-10",
                "valor_a_pagar": "1500000000.00",
                "valor_neto": "1500000000.00",
                "valor_neto_de_la_factura": "1500000000.00",
                "valor_total": "1500000000.00",
                "valor_total_de_la_factura": "1500000000.00",
                "estado": "Pagado",
                "cufe": "CUFECPAY000000000000001",
                "compromiso_presupuestal": "COMP-CPAY-1",
                "codigo_entidad": "800111222",
                "nit_entidad": "800111222",
                "nombre_entidad": "Comprador Uno",
                "referencia_contrato": "REF-PAY-1",
                "fecha_inicio_contrato": "2026-05-09",
                "nombre_proveedor": "Proveedor Pago Alterno SAS",
                "documento_proveedor": "907999999-9",
                "nombre_supervisor": "Supervisor Pago",
                "tipo_documento_supervisor": "CC",
                "documento_supervisor": "1009998887",
            }
        ],
    )
    _write_rows(
        tmp_path,
        "c36g-9fc2",
        [
            {
                "tipoid": "NIT",
                "numeroidentificacion": "907000113-4",
                "codigoprestador": "REPS-907000113",
                "codigohabilitacionsede": "REPS-907000113-SEDE",
                "nombreprestador": "Proveedor Pago Anomalo SAS",
                "nombresede": "Proveedor Pago Anomalo Sede Central",
                "claseprestador": (
                    "Instituciones Prestadoras de Servicios de Salud - IPS"
                ),
                "naturalezajuridica": "Privada",
                "ese": "No",
                "departamentoprestadordesc": "BOGOTA",
                "municipioprestadordesc": "BOGOTA",
                "departamentodededesc": "BOGOTA",
                "municipiosededesc": "BOGOTA",
                "fecha_corte_reps": "2026-05-01",
            }
        ],
    )
    _write_rows(
        tmp_path,
        "epkg-mphw",
        [
            {
                "fecha": "31/12/2026",
                "fecha_corte": "31/12/2026",
                "codigo_departamento": "11",
                "departamento": "BOGOTA",
                "codigo_municipio": "11001",
                "municipio": "BOGOTA",
                "zona_sede": "URBANA",
                "jornada": "Regular",
                "grupo_poblacional": "No pertenece",
                "cantidad_beneficiarios_pae": "12000",
            },
            {
                "fecha": "31/12/2026",
                "fecha_corte": "31/12/2026",
                "codigo_departamento": "11",
                "departamento": "BOGOTA",
                "codigo_municipio": "11001",
                "municipio": "BOGOTA",
                "zona_sede": "RURAL",
                "jornada": "Regular",
                "grupo_poblacional": "Indígenas",
                "cantidad_beneficiarios_pae": "800",
            },
        ],
    )
    _write_rows(
        tmp_path,
        "qddk-cgux",
        [
            {
                "uid": "LEGACY-CPAY-1",
                "contract_signing_year": "2015",
                "entity_name": "Comprador Historico Uno",
                "entity_nit": "800111222",
                "entity_department": "BOGOTA",
                "entity_municipality": "BOGOTA",
                "procurement_modality": "CONTRATACION DIRECTA",
                "contracting_regime_name": "CONTRATACION DIRECTA",
                "process_status": "CELEBRADO",
                "contract_type": "Prestacion de servicios",
                "contract_object": "Servicios de apoyo logistico",
                "contract_object_detail": "Servicios historicos de apoyo",
                "certificate_number": "LEG-CERT-1",
                "process_number": "LEG-PROC-1",
                "contract_number": "LEG-CON-1",
                "award_id": "LEG-AWARD-1",
                "contractor_id_type": "NIT DE PERSONA JURIDICA",
                "contractor_id": "907000113-4",
                "contractor_business_name": "Proveedor Pago Anomalo SAS",
                "legal_rep_doc_type": "CEDULA DE CIUDADANIA",
                "legal_rep_id": "1009998887",
                "legal_rep_name": "Representante Historico",
                "contract_signing_date": "2015-03-01T00:00:00.000",
                "contract_amount": "40000000000",
                "process_amount": "40000000000",
                "total_additions_value": "6000000000",
                "contract_value_with_additions": "46000000000",
                "additions_days": "30",
                "additions_months": "0",
                "additions_marker": "1",
                "process_url_secop_i": (
                    "{'url': 'https://www.contratos.gov.co/legacy/CPAY-1'}"
                ),
            },
            {
                "uid": "LEGACY-CPAY-2",
                "contract_signing_year": "2016",
                "entity_name": "Comprador Historico Dos",
                "entity_nit": "800111333",
                "entity_department": "CUNDINAMARCA",
                "entity_municipality": "SOACHA",
                "procurement_modality": "CONTRATACION DIRECTA MENOR CUANTIA",
                "contracting_regime_name": "CONTRATACION DIRECTA MENOR CUANTIA",
                "process_status": "CELEBRADO",
                "contract_type": "Suministro",
                "contract_object": "Suministro historico",
                "contract_object_detail": "Suministro de bienes historicos",
                "certificate_number": "LEG-CERT-2",
                "process_number": "LEG-PROC-2",
                "contract_number": "LEG-CON-2",
                "award_id": "LEG-AWARD-2",
                "contractor_id_type": "NIT DE PERSONA JURIDICA",
                "contractor_id": "907000113-4",
                "contractor_business_name": "Proveedor Pago Anomalo SAS",
                "contract_signing_date": "2016-04-01T00:00:00.000",
                "contract_amount": "19000000000",
                "process_amount": "19000000000",
                "total_additions_value": "0",
                "contract_value_with_additions": "19000000000",
                "additions_days": "0",
                "additions_months": "0",
                "additions_marker": "0",
                "process_url_secop_i": (
                    "{'url': 'https://www.contratos.gov.co/legacy/CPAY-2'}"
                ),
            },
            {
                "uid": "LEGACY-REP-BRIDGE-1",
                "contract_signing_year": "2014",
                "entity_name": "Comprador Historico Puente",
                "entity_nit": "800444555",
                "entity_department": "BOGOTA",
                "entity_municipality": "BOGOTA",
                "procurement_modality": "CONTRATACION DIRECTA",
                "contracting_regime_name": "CONTRATACION DIRECTA",
                "process_status": "CELEBRADO",
                "contract_type": "Obra",
                "contract_object": "Obra historica representada",
                "contract_object_detail": "Obra historica de infraestructura",
                "certificate_number": "LEG-CERT-REP-1",
                "process_number": "LEG-PROC-REP-1",
                "contract_number": "LEG-CON-REP-1",
                "award_id": "LEG-AWARD-REP-1",
                "contractor_id_type": "NIT DE PERSONA JURIDICA",
                "contractor_id": "908888222-3",
                "contractor_business_name": "Contratista Historico Puente SAS",
                "legal_rep_doc_type": "CEDULA DE CIUDADANIA",
                "legal_rep_id": "333333333",
                "legal_rep_name": "Persona Puente Empresa",
                "contract_signing_date": "2014-02-01T00:00:00.000",
                "contract_amount": "5400000000",
                "process_amount": "5400000000",
                "total_additions_value": "600000000",
                "contract_value_with_additions": "6000000000",
                "additions_days": "15",
                "additions_months": "0",
                "additions_marker": "1",
                "process_url_secop_i": (
                    "{'url': 'https://www.contratos.gov.co/legacy/REP-BRIDGE-1'}"
                ),
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
            },
            {
                "id_contrato": "CLADDER-1",
                "identificador_modificacion": "MOD-LADDER-1",
                "identificador": "MOD-LADDER-1",
                "identificador_requerimiento": "REQ-LADDER-1",
                "valor_modificacion": "0",
                "dias_extendidos": "100",
                "estado_modificacion": "Aprobada",
                "proposito_modificacion": "Prorroga de plazo",
                "descripcion": "Ampliacion de plazo por actividades de obra",
                "codigo_bpin": "BPIN-LADDER-1",
                "fecha_de_aprobacion": "2026-05-20T00:00:00",
                "fecha_version": "2026-05-20T00:00:00",
                "fecha_de_carga": "2026-05-21T00:00:00",
                "fecha_creacion": "2026-05-19T00:00:00",
            },
            {
                "id_contrato": "CLADDER-1",
                "identificador_modificacion": "MOD-LADDER-2",
                "identificador": "MOD-LADDER-2",
                "identificador_requerimiento": "REQ-LADDER-2",
                "valor_modificacion": "0",
                "dias_extendidos": "110",
                "estado_modificacion": "Aprobada",
                "proposito_modificacion": "Modificacion forma de pago",
                "descripcion": "Ajuste a la forma de pago y productos de obra",
                "codigo_bpin": "BPIN-LADDER-1",
                "fecha_de_aprobacion": "2026-06-01T00:00:00",
                "fecha_version": "2026-06-01T00:00:00",
                "fecha_de_carga": "2026-06-02T00:00:00",
                "fecha_creacion": "2026-05-31T00:00:00",
            },
            {
                "id_contrato": "CLADDER-1",
                "identificador_modificacion": "MOD-LADDER-3",
                "identificador": "MOD-LADDER-3",
                "identificador_requerimiento": "REQ-LADDER-3",
                "valor_modificacion": "0",
                "dias_extendidos": "0",
                "estado_modificacion": "Aprobada",
                "proposito_modificacion": "Ajuste de alcance",
                "descripcion": "Modifica alcance, actividades e items del contrato",
                "codigo_bpin": "BPIN-LADDER-1",
                "fecha_de_aprobacion": "2026-06-10T00:00:00",
                "fecha_version": "2026-06-10T00:00:00",
                "fecha_de_carga": "2026-06-11T00:00:00",
                "fecha_creacion": "2026-06-09T00:00:00",
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
                "matricula_category": "SOCIEDAD COMERCIAL",
                "ciiu_primary_activity_code": "7210",
                "matricula_status_code": "01",
                "matricula_status": "ACTIVA",
                "matricula_date": "20200101",
                "fecha_renovacion": "20260301",
                "last_renewed_year": "2026",
                "cancellation_date": "00000000",
                "validity_date": "99991231",
                "update_date": "2026/03/02 00:00:00.000000000",
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
            {
                ":id": "row-c82u-samebuyer-1",
                "document_id": "910111222-6",
                "identification_class": "NIT",
                "business_name": "Proveedor Mismo Representante Uno SAS",
                "matricula": "MAT-SAMEBUYER-1",
                "chamber_of_commerce": "BOGOTA",
                "matricula_status": "ACTIVA",
                "num_identificacion_representante_legal": "444444444",
                "clase_identificacion_rl": "CC",
                "representante_legal": "Representante Mismo Comprador",
            },
            {
                ":id": "row-c82u-samebuyer-2",
                "document_id": "910111333-5",
                "identification_class": "NIT",
                "business_name": "Proveedor Mismo Representante Dos SAS",
                "matricula": "MAT-SAMEBUYER-2",
                "chamber_of_commerce": "BOGOTA",
                "matricula_status": "ACTIVA",
                "num_identificacion_representante_legal": "444444444",
                "clase_identificacion_rl": "CC",
                "representante_legal": "Representante Mismo Comprador",
            },
            {
                ":id": "row-c82u-capacity-1",
                "document_id": "907000113-4",
                "identification_class": "NIT",
                "business_name": "Proveedor Pago Anomalo SAS",
                "matricula": "MAT-CAP-1",
                "chamber_of_commerce": "BOGOTA",
                "matricula_category": "SOCIEDAD COMERCIAL",
                "ciiu_primary_activity_code": "6201",
                "matricula_status_code": "03",
                "matricula_status": "CANCELADA",
                "matricula_date": "20200101",
                "fecha_renovacion": "20240301",
                "last_renewed_year": "2024",
                "cancellation_date": "20260501",
                "update_date": "2026/05/02 00:00:00.000000000",
                "num_identificacion_representante_legal": "333333333",
                "clase_identificacion_rl": "CEDULA DE CIUDADANIA",
                "representante_legal": "Persona Puente Empresa",
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
            {
                "fecha_de_registro": "2026-05-14T09:00:00",
                "referencia_de_la_oferta": "OFFER-RELATED-WINNER",
                "identificador_de_la_oferta": "OFFER-ID-RELATED-WINNER",
                "valor_de_la_oferta": "800000000",
                "entidad_compradora": "Comprador Recurrente",
                "nit_entidad_compradora": "800999888",
                "moneda": "COP",
                "descripcion_del_procedimiento": "Proceso relacionado sintetico",
                "referencia_del_proceso": "REF-PREL-1",
                "id_del_proceso_de_compra": "PREL-1",
                "modalidad": "Licitacion publica",
                "invitacion_directa": "No",
                "nombre_proveedor": "Proveedor Concentrado SAS",
                "nit_del_proveedor": "900765432-6",
                "c_digo_entidad": "ENT-RELATED",
                "c_digo_proveedor": "PROV-RELATED-WINNER",
                ":id": "row-offer-related-winner",
            },
            {
                "fecha_de_registro": "2026-05-14T09:05:00",
                "referencia_de_la_oferta": "OFFER-RELATED-LOSER",
                "identificador_de_la_oferta": "OFFER-ID-RELATED-LOSER",
                "valor_de_la_oferta": "790000000",
                "entidad_compradora": "Comprador Recurrente",
                "nit_entidad_compradora": "800999888",
                "moneda": "COP",
                "descripcion_del_procedimiento": "Proceso relacionado sintetico",
                "referencia_del_proceso": "REF-PREL-1",
                "id_del_proceso_de_compra": "PREL-1",
                "modalidad": "Licitacion publica",
                "invitacion_directa": "No",
                "nombre_proveedor": "Proveedor Recurrente SAS",
                "nit_del_proveedor": "901000111-8",
                "c_digo_entidad": "ENT-RELATED",
                "c_digo_proveedor": "PROV-RELATED-LOSER",
                ":id": "row-offer-related-loser",
            },
            *[
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
                "form_number": "ASSET-1",
                "publication_date": "2026-01-10",
                "declaration_status": "FINALIZADO",
                "declaration_type": "PERIÓDICO",
                "declarant_is_contractor": "NO",
                "declarant_role": "Servidor publico",
                "private_economic_activities": "NO",
                "corp_society_participations": "NO",
                "board_council_participations": "NO",
            }
        ],
    )
    _write_rows(
        tmp_path,
        "gbry-rnq4",
        [
            {
                "document_type": "CEDULA DE CIUDADANIA",
                "document_id": "1001234567",
                "declarant_first_name": "Persona",
                "declarant_second_name": "",
                "declarant_first_lastname": "Conflicto",
                "declarant_second_lastname": "",
                "form_number": "FORM-1",
                "publication_date": "2026-01-15T00:00:00",
                "declaration_status": "FINALIZADO",
                "declaration_type": "PERIÓDICO",
                "entity_name": "Entidad Uno",
                "declarant_is_contractor": "SI",
                "declarant_role": "Contratista",
                "taxable_year": "2025",
                "has_spouse_partner": "NO",
                "spouse_partner": "NO",
                "conflict_relatives": "NO",
                "direct_interest_actions": "SI",
                "conflict_trusts": "NO",
                "other_conflict_investments": "NO",
                "conflict_donations": "NO",
                "other_potential_conflicts": "NO",
                "other_potential_conflicts_desc": "",
            },
            {
                "document_type": "CEDULA DE CIUDADANIA",
                "document_id": "333333333",
                "declarant_first_name": "Persona",
                "declarant_second_name": "",
                "declarant_first_lastname": "Puente",
                "declarant_second_lastname": "Empresa",
                "form_number": "FORM-BRIDGE-1",
                "publication_date": "2026-02-01T00:00:00",
                "declaration_status": "FINALIZADO",
                "declaration_type": "PERIÓDICO",
                "entity_name": "Entidad Puente",
                "declarant_is_contractor": "SI",
                "declarant_role": "Contratista",
                "taxable_year": "2025",
                "has_spouse_partner": "NO",
                "spouse_partner": "NO",
                "conflict_relatives": "NO",
                "direct_interest_actions": "SI",
                "conflict_trusts": "NO",
                "other_conflict_investments": "NO",
                "conflict_donations": "NO",
                "other_potential_conflicts": "NO",
                "other_potential_conflicts_desc": "",
            }
        ],
    )
    _write_rows(
        tmp_path,
        "rpmr-utcd",
        [
            {
                "entity_level": "TERRITORIAL",
                "entity_secop_code": "700001",
                "entity_name": "Comprador Integrado",
                "entity_nit": "800444555",
                "entity_department": "BOGOTA",
                "entity_municipality": "BOGOTA",
                "process_status": "Celebrado",
                "procurement_modality": "Licitacion publica",
                "contract_object": "Obra prioritaria con sancion",
                "process_object": "Obra prioritaria con sancion",
                "contract_type": "Obra",
                "contract_signing_date": "2026-05-01",
                "contract_start_date": "2026-05-02",
                "contract_end_date": "2026-12-31",
                "contract_number": "INT-1",
                "process_number": "PROC-INT-1",
                "contract_value": "150000000",
                "contractor_business_name": "Proveedor Integrado SAS",
                "contract_url": "https://secop-integrado.example/INT-1",
                "origin": "SECOPII",
                "supplier_doc_type": "NIT",
                "supplier_document": "901222333-4",
            },
            *[
                {
                    "entity_level": "TERRITORIAL",
                    "entity_secop_code": "700002",
                    "entity_name": "Comprador PIDA Full",
                    "entity_nit": "800444556",
                    "entity_department": "BOGOTA",
                    "entity_municipality": "BOGOTA",
                    "process_status": "Celebrado",
                    "procurement_modality": "Licitacion publica",
                    "contract_object": object_text,
                    "process_object": object_text,
                    "contract_type": "Obra",
                    "contract_signing_date": f"2026-04-{(index % 28) + 1:02d}",
                    "contract_start_date": f"2026-04-{(index % 28) + 1:02d}",
                    "contract_end_date": "2026-12-31",
                    "contract_number": f"PIDA-FULL-{category}-{index:03d}",
                    "process_number": f"PROC-PIDA-FULL-{category}-{index:03d}",
                    "contract_value": str(
                        30_000_000_000
                        if category == "school_feeding" and index == 0
                        else 20_000_000_000 if index == 0 else 10_000_000_000
                    ),
                    "contractor_business_name": "Proveedor PIDA Full SAS",
                    "contract_url": f"https://secop-integrado.example/PIDA-FULL-{category}-{index:03d}",
                    "origin": "SECOPII",
                    "supplier_doc_type": "NIT",
                    "supplier_document": "901222334-5",
                }
                for category, object_text in [
                    ("school_feeding", "alimentacion escolar PAE"),
                    ("health", "hospital salud medicamentos"),
                    ("water_sanitation", "acueducto y saneamiento"),
                    ("roads_transport", "via vial carretera pavimento"),
                    ("housing", "vivienda habitacional"),
                    ("education", "educacion colegio aula"),
                    ("energy", "energia electrica alumbrado"),
                    ("digital_connectivity", "internet conectividad software"),
                    ("security", "seguridad policia convivencia"),
                    ("sport_culture", "deporte recreacion cultura"),
                    ("environment_risk", "ambiental residuos riesgo"),
                    ("agriculture_rural", "agro rural agricola"),
                ]
                for index in range(42)
            ],
            *[
                {
                    "entity_level": "TERRITORIAL",
                    "entity_secop_code": "700003",
                    "entity_name": "Comprador BPIN Prioritario",
                    "entity_nit": "800444557",
                    "entity_department": "NARINO",
                    "entity_municipality": "TUMACO",
                    "process_status": "Celebrado",
                    "procurement_modality": "Licitacion publica",
                    "contract_object": object_text,
                    "process_object": object_text,
                    "contract_type": "Obra",
                    "contract_signing_date": f"2026-03-0{index}",
                    "contract_start_date": f"2026-03-0{index}",
                    "contract_end_date": "2026-12-31",
                    "contract_number": f"BPIN-PRIO-{index}",
                    "process_number": f"PROC-BPIN-PRIO-{index}",
                    "contract_value": "10000000000",
                    "contractor_business_name": "Proveedor BPIN Prioritario SAS",
                    "contract_url": f"https://secop-integrado.example/BPIN-PRIO-{index}",
                    "origin": "SECOPII",
                    "supplier_doc_type": "NIT",
                    "supplier_document": "901222335-2",
                }
                for index, object_text in [
                    (1, "obra de infraestructura vial prioritaria"),
                    (2, "construccion de acueducto prioritario"),
                    (3, "mejoramiento de hospital prioritario"),
                ]
            ],
        ],
    )
    _write_rows(
        tmp_path,
        "it5q-hg94",
        [
            {
                "process_id": "CO1.BDOS.SECOP-SAN-1",
                "process_reference": "REF-SECOP-SAN-1",
                "contract_id": "SECOP-SAN-1",
                "entity_id": "800111222",
                "entity_name": "Comprador Uno",
                "supplier_code": "905123999",
                "supplier_name": "Proveedor Secop Sancion SAS",
                "amount": "7000000",
                "amount_paid": "7000000",
                "event_date": "2021-06-01",
                "applied_warranties": "True",
                "act_number": "ACT-SECOP-SAN-1",
                "sanction_type": "Multa",
                "description_other_type": "",
                "status": "Publicado por el administrador de la plataforma",
                "type": "Incumplimiento",
                "version_number": "1",
            },
            {
                "process_id": "CO1.BDOS.TEST",
                "process_reference": "REF-SAN-1",
                "contract_id": "INT-1",
                "entity_id": "700001",
                "entity_name": "Comprador Integrado",
                "supplier_code": "901222333",
                "supplier_name": "Proveedor Integrado SAS",
                "amount": "10000000",
                "amount_paid": "10000000",
                "event_date": "2026-05-20",
                "applied_warranties": "False",
                "act_number": "ACT-SAN-1",
                "sanction_type": "Multa",
                "description_other_type": "",
                "status": "Publicado",
                "type": "Incumplimiento",
                "version_number": "1",
            }
        ],
    )
    _write_rows(
        tmp_path,
        "d9na-abhe",
        [
            {
                "codigo_bpin": "202612345678901",
                "anno_bpin": "2026",
                "id_proceso": "REQ-BPIN-1",
                "id_contracto": "BPIN-C-1",
                "id_portafolio": "PORT-BPIN-1",
                "validacion_bpin": "Validado",
            },
            {
                "codigo_bpin": "202612345678901",
                "anno_bpin": "2026",
                "id_proceso": "REQ-BPIN-2",
                "id_contracto": "BPIN-C-2",
                "id_portafolio": "PORT-BPIN-2",
                "validacion_bpin": "Validado",
            },
            {
                "codigo_bpin": "202699990000001",
                "anno_bpin": "2026",
                "id_proceso": "REQ-BPIN-PRIO-1",
                "id_contracto": "BPIN-PRIO-1",
                "id_portafolio": "PORT-BPIN-PRIO-1",
                "validacion_bpin": "Validado",
            },
            {
                "codigo_bpin": "202699990000001",
                "anno_bpin": "2026",
                "id_proceso": "REQ-BPIN-PRIO-2",
                "id_contracto": "BPIN-PRIO-2",
                "id_portafolio": "PORT-BPIN-PRIO-2",
                "validacion_bpin": "Validado",
            },
            {
                "codigo_bpin": "202699990000001",
                "anno_bpin": "2026",
                "id_proceso": "REQ-BPIN-PRIO-3",
                "id_contracto": "BPIN-PRIO-3",
                "id_portafolio": "PORT-BPIN-PRIO-3",
                "validacion_bpin": "Validado",
            },
        ],
    )
    _write_rows(
        tmp_path,
        "qkv4-ek54",
        [
            {
                "period": "20260901",
                "entity_code": "700001",
                "entity_name": "Gobernacion Proyecto SGR",
                "account": "2.3.2.02",
                "account_name": "Inversion",
                "scope_code": "A461",
                "cpc_code": "0",
                "cpc_name": "NO APLICA",
                "sector_code": "24",
                "sector_name": "Transporte",
                "funding_sources_code": "4",
                "funding_sources_name": "Asignaciones directas",
                "resource_type_code": "2",
                "resource_type_name": "Disponibilidad inicial",
                "mga_program_code": "2402",
                "mga_program_name": "Infraestructura vial",
                "bpin": "202612345678901",
                "thirdparty_code": "700001",
                "thirdparty_name": "Gobernacion Proyecto SGR",
                "public_policy_code": "0",
                "public_policy_name": "NO APLICA",
                "fund_situation_code": "1",
                "fund_situation_name": "Con situacion de fondos",
                "commitments": "60000000000",
                "obligations": "55000000000",
                "payments": "52000000000",
            }
        ],
    )
    _write_rows(
        tmp_path,
        "mzgh-shtp",
        [
            {
                "codigobpin": "202612345678901",
                "nombre": "Proyecto SGR con contratacion validada",
                "estado": "EJECUCION",
                "valortotal": "130000000000.00",
                "codejecutor": "700001",
                "entidadejecutora": "Gobernacion Proyecto SGR",
                "departamento": "BOGOTA",
                "ejecucionfinanciera": "42",
                "nomocad": "OCAD TEST",
                "interventor": "Interventor Test",
                "proyecto_paz": "NO",
                "proyecto_grupo_etnico": "Sin Enfoque Diferencial",
                "proyecto_covid": "NO",
                "sector": "Transporte",
                "ejecucionfisica": "38",
            }
        ],
    )
    _write_rows(
        tmp_path,
        "epzv-8ck4",
        [
            {
                "bpin": "202612345678901",
                "nombreproyecto": "Proyecto SGR con contratacion validada",
                "codigoentidadejecutora": "700001",
                "entidadejecutora": "Gobernacion Proyecto SGR",
            }
        ],
    )
    _write_rows(
        tmp_path,
        "xikz-44ja",
        [
            {
                "bpin": "202612345678901",
                "nombreproyecto": "Proyecto SGR con contratacion validada",
                "region": "CENTRO ORIENTE",
                "codigodepartamento": "11",
                "departamento": "BOGOTA",
                "codigomunicipio": "11001",
                "municipio": "BOGOTA",
                "entidadresponsable": "Gobernacion Proyecto SGR",
                "sector": "Transporte",
                "idregion": "R1",
                "codigoentidadresponsable": "700001",
            }
        ],
    )
    _write_rows(
        tmp_path,
        "iuc2-3r6h",
        [
            {
                "bpin": "202612345678901",
                "nombreproyecto": "Proyecto SGR con contratacion validada",
                "departamento": "BOGOTA",
                "municipio": "BOGOTA",
                "totalbeneficiario": "0",
                "sector": "Transporte",
                "entidadresponsable": "Gobernacion Proyecto SGR",
            }
        ],
    )
    _write_rows(
        tmp_path,
        "tmmn-mpqc",
        [
            {
                "bpin": "202612345678901",
                "nombreproyecto": "Proyecto SGR con contratacion validada",
                "cantidad": "12000",
                "sector": "Transporte",
                "entidadresponsable": "Gobernacion Proyecto SGR",
                "totalbeneficiario": "0",
                "caracteristicademografica": "Personas con discapacidad",
            }
        ],
    )
    _write_rows(
        tmp_path,
        "3hdv-smhz",
        [
            {
                "order_id": f"TVEC-{index}",
                "order_date": f"2026-05-{(index % 28) + 1:02d}",
                "buyer_name": f"Comprador TVEC {index % 50}",
                "buyer_nit": f"810{index % 50:06d}",
                "supplier_name": "Proveedor Red Densa SAS",
                "supplier_nit": "906000111-6",
                "item_name": "Item TVEC QA",
                "unit_price": "10000000",
                "quantity": "1",
                "unit": "Unidad",
                "line_total": str(20_000_000 if index == 0 else 10_000_000),
                "cdp": f"CDP-TVEC-{index}",
            }
            for index in range(100)
        ]
        + [
            {
                "order_id": f"TVEC-PRICE-{index}",
                "order_date": f"2026-05-{(index % 28) + 1:02d}",
                "buyer_name": f"Comprador Precio {index % 5}",
                "buyer_nit": f"82000000{index % 5}",
                "supplier_name": f"Proveedor Precio {index % 3}",
                "supplier_nit": [
                    "910000001-1",
                    "910000002-2",
                    "910000003-3",
                ][index % 3],
                "item_name": "Termometro digital QA",
                "unit_price": "100" if index < 15 else "300" if index < 19 else "500",
                "quantity": "1",
                "unit": "Unidad",
                "line_total": "100" if index < 15 else "300" if index < 19 else "500",
                "cdp": f"CDP-PRICE-{index}",
            }
            for index in range(20)
        ],
    )
    _write_rows(
        tmp_path,
        "s484-c9k3",
        [
            {
                "contract_id": "IA-1",
                "load_year": "2026",
                "signing_year": "2026",
                "load_date": "2026-05-20T00:00:00.000",
                "signing_date": "2026-05-15T00:00:00.000",
                "contract_start_date": "2026-05-16T00:00:00.000",
                "contract_end_date": "2026-12-31T00:00:00.000",
                "government_order": "TERRITORIAL",
                "department": "BOGOTA",
                "municipality": "BOGOTA",
                "entity_name": "Comprador Interadministrativo",
                "procurement_modality": "CONTRATOS Y CONVENIOS CON MAS DE DOS PARTES",
                "contract_type": "OTRO TIPO DE CONTRATO",
                "modality_justification": "Convenio interadministrativo",
                "unspsc_class_id": "SERVICIOS DE ADMINISTRACION PUBLICA",
                "process_id": "P-IA-1",
                "contract_number": "IA-2026-001",
                "contractual_object": (
                    "Convenio interadministrativo para ejecutar servicios "
                    "territoriales con alto anticipo"
                ),
                "amount_with_additions": "12000000000",
                "contractor_name": "Proveedor Pago Anomalo SAS",
                "contractor_type": "NIT DE PERSONA JURIDICA",
                "contractor_id": "907000113-4",
                "contract_status": "CELEBRADO",
                "link": "https://secop.example/IA-1",
                "entity_id": "ENT-IA",
                "source_system": "SECOP_I",
                "resource_origin": "RECURSOS PROPIOS",
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
        "signal_feature_procurement_contract_modification_ladder_review_only",
        "signal_feature_procurement_sanctioned_supplier_awarded",
        "signal_feature_procurement_secop_sanction_later_awards_review_only",
        "signal_feature_fiscal_procurement_chronology_review_only",
        "signal_feature_siri_antecedent_procurement_chronology_review_only",
        "signal_feature_procurement_supplier_concentration_across_entities",
        "signal_feature_procurement_contract_value_outlier_by_category",
        "signal_feature_procurement_repeat_awards_same_supplier",
        "signal_feature_procurement_buyer_supplier_network_density",
        "signal_feature_procurement_cartel_risk_cobidding",
        "signal_feature_procurement_related_bidders_same_process_review_only",
        "signal_feature_procurement_shared_representative_same_buyer_cluster_review_only",
        "signal_feature_procurement_payment_plan_anomalies",
        "signal_feature_procurement_guarantee_advance_execution_chain",
        "signal_feature_procurement_guarantee_policy_reuse_review_only",
        "signal_feature_procurement_budget_chain_reconciliation_review_only",
        "signal_feature_procurement_invoice_budget_reconciliation_review_only",
        "signal_feature_procurement_payment_plan_reconciliation_review_only",
        "signal_feature_health_pae_service_delivery_gap_review_only",
        "signal_feature_pae_beneficiary_territory_delivery_gap_review_only",
        "signal_feature_procurement_contract_suspensions",
        "signal_feature_procurement_contract_execution_delay",
        "signal_feature_project_bpin_procurement_overlap",
        "signal_feature_project_regalias_execution_procurement_overlap",
        "signal_feature_sgr_ocad_executor_capacity_gap",
        "signal_feature_dnp_sgr_beneficiary_delivery_gap_review_only",
        "signal_feature_bpin_dnp_vs_pida27_obras_prioritarias",
        "signal_feature_procurement_short_bidding_window",
        "signal_feature_procurement_offers_competition_drop",
        "signal_feature_procurement_public_servant_conflict_disclosure_overlap",
        "signal_feature_procurement_role_supplier_same_buyer_review_only",
        "signal_feature_public_declaration_supplier_chronology_review_only",
        "signal_feature_public_declaration_company_bridge_current_risk_review_only",
        "signal_feature_cuentas_claras_donor_supplier_overlap",
        "signal_feature_cuentas_claras_donor_ineligibility_review",
        "signal_feature_pida_full30_meta",
        "signal_feature_pida5_pida27_pida4_chain",
        "signal_feature_procurement_politically_exposed_position_supplier_overlap",
        "signal_feature_tvec_multi_entity_capture",
        "signal_feature_tvec_item_price_dispersion_review_only",
        "signal_feature_procurement_related_companies_shared_officer",
        "signal_feature_procurement_cross_source_identity_inconsistency",
        "signal_feature_rues_supplier_capacity_status_review_only",
        "signal_feature_cross_signal_compound_risk_review_only",
        "signal_feature_secop_i_legacy_supplier_current_risk_review_only",
        "signal_feature_secop_i_legacy_representative_current_risk_review_only",
        "signal_feature_secop_interadmin_executor_network_review_only",
    }
    rows_by_table = {result.table: result.rows for result in results}
    assert rows_by_table["fct_procurement_contract_awards"] == 233
    assert rows_by_table["dim_company"] == 12
    assert rows_by_table["dim_buyer"] == 57
    assert rows_by_table["dim_person"] == 1
    assert rows_by_table["signal_feature_procurement_single_bidder_high_value"] == 1
    assert rows_by_table["signal_feature_procurement_large_modifications"] == 1
    assert (
        rows_by_table[
            "signal_feature_procurement_contract_modification_ladder_review_only"
        ]
        == 1
    )
    assert rows_by_table["signal_feature_procurement_sanctioned_supplier_awarded"] == 1
    assert (
        rows_by_table[
            "signal_feature_procurement_secop_sanction_later_awards_review_only"
        ]
        == 1
    )
    assert (
        rows_by_table["signal_feature_fiscal_procurement_chronology_review_only"]
        == 2
    )
    assert (
        rows_by_table[
            "signal_feature_siri_antecedent_procurement_chronology_review_only"
        ]
        == 5
    )
    assert rows_by_table["signal_feature_procurement_supplier_concentration_across_entities"] == 1
    assert rows_by_table["signal_feature_procurement_contract_value_outlier_by_category"] == 1
    assert rows_by_table["signal_feature_procurement_repeat_awards_same_supplier"] == 1
    assert rows_by_table["signal_feature_procurement_buyer_supplier_network_density"] == 1
    assert rows_by_table["signal_feature_procurement_cartel_risk_cobidding"] == 2
    assert (
        rows_by_table[
            "signal_feature_procurement_related_bidders_same_process_review_only"
        ]
        == 1
    )
    assert (
        rows_by_table[
            "signal_feature_procurement_shared_representative_same_buyer_cluster_review_only"
        ]
        == 2
    )
    assert rows_by_table["signal_feature_procurement_payment_plan_anomalies"] == 1
    assert (
        rows_by_table[
            "signal_feature_procurement_guarantee_advance_execution_chain"
        ]
        == 1
    )
    assert (
        rows_by_table[
            "signal_feature_procurement_guarantee_policy_reuse_review_only"
        ]
        == 2
    )
    assert (
        rows_by_table[
            "signal_feature_procurement_budget_chain_reconciliation_review_only"
        ]
        == 5
    )
    assert (
        rows_by_table[
            "signal_feature_procurement_invoice_budget_reconciliation_review_only"
        ]
        == 1
    )
    assert (
        rows_by_table[
            "signal_feature_procurement_payment_plan_reconciliation_review_only"
        ]
        == 1
    )
    assert (
        rows_by_table["signal_feature_health_pae_service_delivery_gap_review_only"]
        == 1
    )
    assert (
        rows_by_table[
            "signal_feature_pae_beneficiary_territory_delivery_gap_review_only"
        ]
        == 1
    )
    assert rows_by_table["signal_feature_procurement_contract_suspensions"] == 1
    assert rows_by_table["signal_feature_procurement_contract_execution_delay"] == 1
    assert rows_by_table["signal_feature_project_bpin_procurement_overlap"] == 1
    assert (
        rows_by_table["signal_feature_project_regalias_execution_procurement_overlap"]
        == 1
    )
    assert rows_by_table["signal_feature_sgr_ocad_executor_capacity_gap"] == 1
    assert (
        rows_by_table[
            "signal_feature_dnp_sgr_beneficiary_delivery_gap_review_only"
        ]
        == 1
    )
    assert rows_by_table["signal_feature_bpin_dnp_vs_pida27_obras_prioritarias"] == 1
    assert rows_by_table["signal_feature_procurement_short_bidding_window"] == 1
    assert rows_by_table["signal_feature_procurement_offers_competition_drop"] == 1
    assert (
        rows_by_table[
            "signal_feature_procurement_public_servant_conflict_disclosure_overlap"
        ]
        == 1
    )
    assert (
        rows_by_table["signal_feature_procurement_role_supplier_same_buyer_review_only"]
        == 1
    )
    assert (
        rows_by_table[
            "signal_feature_public_declaration_supplier_chronology_review_only"
        ]
        == 2
    )
    assert (
        rows_by_table[
            "signal_feature_public_declaration_company_bridge_current_risk_review_only"
        ]
        == 1
    )
    assert rows_by_table["signal_feature_cuentas_claras_donor_supplier_overlap"] == 1
    assert (
        rows_by_table["signal_feature_cuentas_claras_donor_ineligibility_review"]
        == 1
    )
    assert rows_by_table["signal_feature_pida_full30_meta"] == 1
    assert rows_by_table["signal_feature_pida5_pida27_pida4_chain"] == 1
    assert rows_by_table["signal_feature_tvec_multi_entity_capture"] == 1
    assert rows_by_table["signal_feature_tvec_item_price_dispersion_review_only"] == 1
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
    assert rows_by_table["signal_feature_rues_supplier_capacity_status_review_only"] == 1
    assert rows_by_table["signal_feature_cross_signal_compound_risk_review_only"] == 3
    assert (
        rows_by_table[
            "signal_feature_secop_i_legacy_supplier_current_risk_review_only"
        ]
        == 1
    )
    assert (
        rows_by_table[
            "signal_feature_secop_i_legacy_representative_current_risk_review_only"
        ]
        == 1
    )
    assert rows_by_table["signal_feature_secop_interadmin_executor_network_review_only"] == 1

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
        secop_sanction_later_rows = con.execute(
            "SELECT entity_key, scope_key, severity, sanction_contract_id, "
            "sanction_type, sanction_status, later_contract_count, "
            "later_buyer_count, later_contract_value, "
            "same_buyer_later_contract_count, same_buyer_later_contract_value, "
            "direct_or_exception_later_contract_count, evidence_refs[1], "
            "evidence_refs[2], evidence_refs[3] FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_secop_sanction_later_awards_review_only"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        fiscal_chronology_rows = con.execute(
            "SELECT entity_key, fiscal_source_type, fiscal_record_id, severity, "
            "later_contract_count, later_contract_value, fiscal_amount, "
            "evidence_refs[1], evidence_refs[2] "
            "FROM read_parquet(?) ORDER BY fiscal_source_type",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_fiscal_procurement_chronology_review_only"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        siri_rows = con.execute(
            "SELECT entity_key, exposure_type, contract_id, severity, "
            "status_bucket, active_period_overlap_flag, contract_value, "
            "evidence_refs[1], evidence_refs[2] "
            "FROM read_parquet(?) ORDER BY exposure_type, contract_id",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_siri_antecedent_procurement_chronology_review_only"
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
        guarantee_chain_rows = con.execute(
            "SELECT entity_key, scope_key, severity, guarantee_source_status, "
            "contract_value, advance_payment_value, round(advance_payment_share, 2), "
            "pending_execution_value, round(pending_execution_share, 2), "
            "high_advance_payment_flag, ended_pending_execution_flag, suspension_flag, "
            "large_modification_flag, secop_sanction_flag, chain_flag_count, "
            "suspension_event_count, guarantee_row_count, guarantee_accepted_count, "
            "guarantee_rejected_count, guarantee_sent_after_start_flag, "
            "guarantee_value_below_advance_flag, guarantee_issue_flag_count, "
            "evidence_refs[1], evidence_refs[2], evidence_refs[4] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_guarantee_advance_execution_chain"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        guarantee_policy_reuse_rows = con.execute(
            "SELECT entity_key, scope_key, severity, policy_reuse_status, "
            "contract_id, insurer, policy_number, guarantee_status, contract_value, "
            "cluster_contract_count, cluster_supplier_count, cluster_buyer_count, "
            "cluster_accepted_contract_count, cluster_expired_contract_count, "
            "cluster_total_contract_value, cluster_max_contract_value, "
            "policy_like_number_flag, different_supplier_policy_reuse_flag, "
            "different_buyer_policy_reuse_flag, small_policy_cluster_flag, "
            "all_cluster_contracts_accepted_or_expired_flag, high_cluster_value_flag, "
            "very_high_cluster_value_flag, evidence_refs[1], evidence_refs[2], "
            "evidence_refs[3], evidence_refs[4] "
            "FROM read_parquet(?) ORDER BY entity_key",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_guarantee_policy_reuse_review_only"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        budget_chain_rows = con.execute(
            "SELECT entity_key, scope_key, severity, budget_issue_flag_count, "
            "missing_cdp_flag, missing_commitment_flag, missing_rubro_flag, "
            "cdp_only_weak_siif_flag, commitment_only_failed_flag, "
            "cdp_under_contract_flag, commitment_under_contract_flag, "
            "commitment_over_contract_flag, rubro_zero_or_undefined_flag, "
            "guarantee_chain_flag, contract_value, cdp_used_value, "
            "commitment_value, rubro_value, evidence_refs[1], evidence_refs[2], "
            "evidence_refs[3], evidence_refs[4], evidence_refs[5] "
            "FROM read_parquet(?) WHERE scope_key = 'CPAY-1'",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_budget_chain_reconciliation_review_only"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        invoice_budget_rows = con.execute(
            "SELECT entity_key, scope_key, severity, anomaly_type, "
            "invoice_issue_flag_count, invoice_value_over_contract_flag, "
            "confirmed_invoice_over_contract_flag, invoice_after_contract_end_flag, "
            "invoice_summary_zero_gap_flag, budget_chain_flag, guarantee_chain_flag, "
            "contract_value, invoice_source_value, confirmed_invoice_value, "
            "post_end_invoice_value, invoice_excess_value, evidence_refs[1], "
            "evidence_refs[2], evidence_refs[3], evidence_refs[4] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / (
                        "table=signal_feature_procurement_invoice_budget_reconciliation"
                        "_review_only"
                    )
                    / "*.parquet"
                )
            ],
        ).fetchall()
        payment_plan_reconciliation_rows = con.execute(
            "SELECT entity_key, scope_key, severity, anomaly_type, "
            "payment_issue_flag_count, actual_paid_over_contract_flag, "
            "real_payment_after_contract_end_flag, "
            "payment_before_invoice_sequence_flag, "
            "payment_supplier_document_mismatch_flag, "
            "duplicate_cufe_cross_contract_flag, payment_summary_zero_gap_flag, "
            "budget_chain_flag, guarantee_chain_flag, invoice_chain_flag, "
            "contract_value, actual_paid_value, post_end_actual_paid_value, "
            "actual_paid_excess_value, supplier_mismatch_paid_value, "
            "payment_before_invoice_issue_value, payment_before_invoice_receipt_value, "
            "evidence_refs[1], evidence_refs[2], evidence_refs[3], "
            "evidence_refs[4], evidence_refs[5] FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / (
                        "table=signal_feature_procurement_payment_plan_reconciliation"
                        "_review_only"
                    )
                    / "*.parquet"
                )
            ],
        ).fetchall()
        health_pae_rows = con.execute(
            "SELECT entity_key, scope_key, severity, domain_type, "
            "provider_match_flag, health_keyword_flag, provider_class, "
            "support_signal_count, support_family_count, support_signal_families, "
            "support_signal_ids, evidence_refs[1], evidence_refs[2], "
            "evidence_refs[3] FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_health_pae_service_delivery_gap_review_only"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        pae_beneficiary_rows = con.execute(
            "SELECT entity_key, scope_key, severity, domain_type, contract_id, "
            "pae_year, pae_same_year_flag, pae_beneficiary_count, "
            "pae_group_beneficiary_count, pae_vulnerable_beneficiary_count, "
            "pae_rural_beneficiary_count, pae_population_group_count, "
            "support_signal_count, support_family_count, evidence_refs[1], "
            "evidence_refs[2], evidence_refs[8] FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / (
                        "table=signal_feature_pae_beneficiary_territory_delivery_gap"
                        "_review_only"
                    )
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
        modification_ladder_rows = con.execute(
            "SELECT entity_key, scope_key, severity, contract_value, "
            "modification_event_count, total_modification_value, "
            "total_extended_days, chain_flag_count, support_signal_count, "
            "material_value_flag, major_delay_flag, financial_terms_flag, "
            "scope_change_flag, modality_context_flag, evidence_refs[1], "
            "evidence_refs[2], evidence_refs[3], evidence_refs[4] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / (
                        "table=signal_feature_procurement_contract_modification_ladder"
                        "_review_only"
                    )
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
        bpin_project_rows = con.execute(
            "SELECT entity_key, scope_key, severity, bpin_year, contract_count, "
            "supplier_count, buyer_count, total_contract_value, evidence_refs[1], "
            "evidence_refs[3] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_project_bpin_procurement_overlap"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        regalias_project_rows = con.execute(
            "SELECT entity_key, scope_key, severity, project_title, contract_count, "
            "total_contract_value, expense_row_count, commitments_total, "
            "max_sgr_execution_value, evidence_refs[1], evidence_refs[2], "
            "evidence_refs[3], evidence_refs[5] FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_project_regalias_execution_procurement_overlap"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        sgr_ocad_rows = con.execute(
            "SELECT entity_key, scope_key, severity, project_title, project_total_value, "
            "total_contract_value, payments_total, financial_execution_pct, "
            "physical_execution_pct, capacity_gap_flag_count, "
            "low_project_execution_flag, paid_low_physical_flag, "
            "contract_low_physical_flag, procurement_failure_flag, "
            "rues_capacity_flag, evidence_refs[1], evidence_refs[2], "
            "evidence_refs[3], evidence_refs[5] FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_sgr_ocad_executor_capacity_gap"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        dnp_sgr_rows = con.execute(
            "SELECT entity_key, scope_key, severity, dnp_executor_count, "
            "beneficiary_territory_count, demographic_observation_count, "
            "vulnerable_demographic_observation_count, dnp_delivery_flag_count, "
            "dnp_delivery_gap_types, evidence_refs[7], evidence_refs[10] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_dnp_sgr_beneficiary_delivery_gap_review_only"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        compound_risk_rows = con.execute(
            "SELECT entity_label, entity_key, severity, signal_count, family_count, "
            "feature_row_count, signal_families, signal_ids, evidence_refs[1], "
            "evidence_refs[2] FROM read_parquet(?) ORDER BY entity_key",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_cross_signal_compound_risk_review_only"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        secop_i_legacy_rows = con.execute(
            "SELECT entity_key, scope_key, severity, base_cross_signal_severity, "
            "current_signal_count, current_family_count, legacy_contract_count, "
            "legacy_buyer_count, legacy_total_contract_value, "
            "legacy_total_addition_value, legacy_material_addition_count, "
            "legacy_direct_or_exception_count, evidence_refs[1], "
            "list_contains(evidence_refs, 'secop_i_historical_processes:LEGACY-CPAY-1') "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / (
                        "table=signal_feature_secop_i_legacy_supplier_current_risk"
                        "_review_only"
                    )
                    / "*.parquet"
                )
            ],
        ).fetchall()
        secop_i_legacy_representative_rows = con.execute(
            "SELECT entity_key, scope_key, severity, base_cross_signal_severity, "
            "current_signal_count, current_family_count, representative_document_key, "
            "representative_document_id, legacy_representative_document_id, "
            "representative_current_risk_company_count, legacy_contractor_count, "
            "legacy_contract_count, legacy_buyer_count, legacy_total_contract_value, "
            "legacy_total_addition_value, legacy_material_addition_count, "
            "legacy_direct_or_exception_count, evidence_refs[1], "
            "list_contains(evidence_refs, "
            "'secop_i_historical_processes:LEGACY-REP-BRIDGE-1'), "
            "list_contains(evidence_refs, "
            "'company_registry_c82u:row-c82u-capacity-1') "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / (
                        "table=signal_feature_secop_i_legacy_representative_current"
                        "_risk_review_only"
                    )
                    / "*.parquet"
                )
            ],
        ).fetchall()
        interadmin_rows = con.execute(
            "SELECT entity_key, scope_key, severity, contractor_name, agreement_id, "
            "agreement_value, support_signal_count, support_family_count, "
            "support_signal_families, support_signal_ids, evidence_refs[1], "
            "evidence_refs[2] FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_secop_interadmin_executor_network_review_only"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        bpin_priority_rows = con.execute(
            "SELECT entity_key, scope_key, severity, department, municipality, "
            "priority_contract_count, priority_category_count, "
            "priority_contract_value, evidence_refs[1], evidence_refs[6] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_bpin_dnp_vs_pida27_obras_prioritarias"
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
        conflict_disclosure_rows = con.execute(
            "SELECT entity_key, scope_key, severity, form_number, conflict_flag_count, "
            "contract_count, buyer_count, total_contract_value, evidence_refs[1], "
            "evidence_refs[2] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_public_servant_conflict_disclosure_overlap"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        role_supplier_rows = con.execute(
            "SELECT entity_key, scope_key, severity, role_field, buyer_document_id, "
            "role_contract_count, supplier_contract_count, supplier_total_contract_value, "
            "evidence_refs[1], evidence_refs[2] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_role_supplier_same_buyer_review_only"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        declaration_supplier_rows = con.execute(
            "SELECT entity_key, scope_key, severity, buyer_document_id, "
            "pair_contract_count, pair_total_contract_value, conflict_flag_count, "
            "same_entity_contract_count, evidence_refs[1], evidence_refs[2] "
            "FROM read_parquet(?) ORDER BY buyer_document_id",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_public_declaration_supplier_chronology_review_only"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        declaration_company_bridge_rows = con.execute(
            "SELECT entity_key, scope_key, severity, base_cross_signal_severity, "
            "current_signal_count, current_family_count, representative_document_key, "
            "person_document_id, conflict_disclosure_count, conflict_flag_count, "
            "conflict_contractor_flag, evidence_refs[1], "
            "list_contains(evidence_refs, 'conflict_disclosures:FORM-BRIDGE-1'), "
            "list_contains(evidence_refs, 'company_registry_c82u:row-c82u-capacity-1') "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / (
                        "table=signal_feature_public_declaration_company_bridge"
                        "_current_risk_review_only"
                    )
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
        cuentas_ineligibility_rows = con.execute(
            "SELECT entity_key, scope_key, severity, office_level, "
            "jurisdiction_match_type, threshold_status, election_result_status, "
            "term_status, total_income_amount, term_contract_count, "
            "term_contract_value, buyer_document_digits, evidence_refs[1], "
            "evidence_refs[2] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_cuentas_claras_donor_ineligibility_review"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        pida_chain_rows = con.execute(
            "SELECT entity_key, scope_key, severity, department, municipality, "
            "sanction_event_count, sanctioned_contract_count, "
            "sanctioned_contract_value, evidence_refs[1], evidence_refs[2] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_pida5_pida27_pida4_chain"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        pida_full_rows = con.execute(
            "SELECT entity_key, scope_key, severity, department, municipality, "
            "contract_count, pida_category_count, total_contract_value, "
            "very_high_value_contract_count, evidence_refs[1] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_pida_full30_meta"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        tvec_capture_rows = con.execute(
            "SELECT entity_key, scope_key, severity, tvec_order_count, "
            "tvec_buyer_count, tvec_total_value, secop_contract_count, "
            "secop_total_contract_value, evidence_refs[1], evidence_refs[6] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_tvec_multi_entity_capture"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        tvec_price_rows = con.execute(
            "SELECT entity_key, scope_key, severity, order_id, buyer_document_digits, "
            "item_key, unit_key, quantity_band, year_bucket, observed_unit_price, "
            "high_price_line_value, comparable_line_count, comparable_supplier_count, "
            "comparable_buyer_count, median_unit_price, p90_unit_price, "
            "round(p95_unit_price, 2), evidence_refs[1], evidence_refs[2] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_tvec_item_price_dispersion_review_only"
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
        related_bidders_rows = con.execute(
            "SELECT entity_key, scope_key, severity, process_key, contract_id, "
            "contract_value, related_offerer_document_key, "
            "related_offerer_name, related_offer_value, process_supplier_count, "
            "repeated_pair_process_count, representative_document_key, "
            "bounded_competition_flag, repeated_pair_flag, very_high_value_flag, "
            "strpos(what_is_unproven, 'does not prove collusion') > 0, "
            "evidence_refs[1], evidence_refs[2], "
            "evidence_refs[3], evidence_refs[4] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_procurement_related_bidders_same_process_review_only"
                    / "*.parquet"
                )
            ],
        ).fetchall()
        shared_representative_same_buyer_rows = con.execute(
            "SELECT entity_key, scope_key, severity, buyer_document_id, "
            "signing_year, representative_document_key, cluster_supplier_count, "
            "cluster_contract_count, cluster_total_contract_value, "
            "cluster_direct_or_exception_count, supplier_contract_count, "
            "supplier_contract_value, evidence_refs[1], "
            "list_contains(evidence_refs, "
            "'https://secop.example/CSHARED-1'), "
            "list_contains(evidence_refs, "
            "'https://secop.example/CSHARED-3'), "
            "list_contains(evidence_refs, "
            "'company_registry_c82u:row-c82u-samebuyer-1'), "
            "list_contains(evidence_refs, "
            "'company_registry_c82u:row-c82u-samebuyer-2') "
            "FROM read_parquet(?) ORDER BY entity_key",
            [
                str(
                    tmp_path
                    / "curated"
                    / (
                        "table=signal_feature_procurement_shared_representative"
                        "_same_buyer_cluster_review_only"
                    )
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
        rues_capacity_rows = con.execute(
            "SELECT entity_key, scope_key, severity, supplier_name, company_name, "
            "contract_count, buyer_count, total_contract_value, max_contract_value, "
            "has_active_registry_row, has_inactive_registry_row, "
            "cast(latest_cancellation_date AS VARCHAR), last_renewed_year, "
            "evidence_matricula_status, "
            "post_inactive_contract_flag, stale_renewal_high_value_flag, "
            "recent_registration_large_award_flag, multi_matricula_supplier_flag, "
            "capacity_flag_count, evidence_refs[1], evidence_refs[2] "
            "FROM read_parquet(?)",
            [
                str(
                    tmp_path
                    / "curated"
                    / "table=signal_feature_rues_supplier_capacity_status_review_only"
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
            0.75,
            "https://secop.example/C-1",
        )
    ]
    assert secop_sanction_later_rows == [
        (
            "905123999",
            "secop_sanction_later_awards:SECOP-SAN-1:905123999:1",
            "high",
            "SECOP-SAN-1",
            "Multa",
            "Publicado por el administrador de la plataforma",
            1,
            1,
            150_000_000.0,
            1,
            150_000_000.0,
            1,
            "secop_sanctions:ACT-SECOP-SAN-1",
            "https://secop.example/SECOP-SAN-1",
            "https://secop.example/SECOP-SAN-LATER",
        )
    ]
    assert fiscal_chronology_rows == [
        (
            "900123456",
            "fiscal_finding",
            "HF-1:1",
            "medium",
            1,
            125_000_000.0,
            50_000_000.0,
            "fiscal_findings:HF-1:1",
            "https://secop.example/C-1",
        ),
        (
            "900123456",
            "fiscal_responsibility",
            "RF-2020-1:1",
            "critical",
            1,
            125_000_000.0,
            150_000_000.0,
            "fiscal_responsibility:RF-2020-1:1",
            "https://secop.example/C-1",
        ),
    ]
    assert siri_rows == [
        (
            "1001234567",
            "direct_supplier",
            "C-2",
            "high",
            "active_ineligibility_inferred",
            True,
            45_000_000.0,
            "siri_antecedents:SIRI-1",
            "https://secop.example/C-2",
        ),
        (
            "1001234567",
            "direct_supplier",
            "CPER-1",
            "high",
            "active_ineligibility_inferred",
            True,
            800_000_000.0,
            "siri_antecedents:SIRI-1",
            "https://secop.example/CPER-1",
        ),
        (
            "1001234567",
            "direct_supplier",
            "CPER-2",
            "high",
            "active_ineligibility_inferred",
            True,
            750_000_000.0,
            "siri_antecedents:SIRI-1",
            "https://secop.example/CPER-2",
        ),
        (
            "1001234567",
            "direct_supplier",
            "CPER-3",
            "high",
            "active_ineligibility_inferred",
            True,
            500_000_000.0,
            "siri_antecedents:SIRI-1",
            "https://secop.example/CPER-3",
        ),
        (
            "1001234567",
            "role_supervisor",
            "C-1",
            "critical",
            "active_ineligibility_inferred",
            True,
            125_000_000.0,
            "siri_antecedents:SIRI-1",
            "secop_ii_contracts:role:C-1:supervisor_doc_number",
        ),
    ]
    assert concentration_rows == [
        ("900765432", 51, 51, 5_800_000_000.0, "https://secop.example/CREL-1")
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
    assert guarantee_chain_rows == [
        (
            "907000113",
            "CPAY-1",
            "high",
            "accepted_guarantee_present_but_sent_after_start",
            1_000_000_000.0,
            600_000_000.0,
            0.6,
            1_000_000_000.0,
            1.0,
            True,
            True,
            True,
            False,
            False,
            3,
            1,
            2,
            1,
            1,
            True,
            True,
            3,
            "https://secop.example/CPAY-1",
            "secop_guarantees:CPAY-1:POL-REJECTED:1",
            "secop_contract_suspensions:CPAY-1:2026-05-15",
        )
    ]
    assert guarantee_policy_reuse_rows == [
        (
            "909000111",
            "guarantee_policy_reuse:segurosmundial:100777888:CREUSE-1",
            "high",
            "same_policy_two_contracts_different_supplier_buyer",
            "CREUSE-1",
            "SEGUROS MUNDIAL",
            "100777888",
            "Aceptada",
            600_000_000.0,
            2,
            2,
            2,
            1,
            1,
            1_300_000_000.0,
            700_000_000.0,
            True,
            True,
            True,
            True,
            True,
            True,
            False,
            "https://secop.example/CREUSE-1",
            "secop_guarantees:CREUSE-1:100777888:1",
            "https://secop.example/CREUSE-2",
            "secop_guarantees:CREUSE-2:100777888:1",
        ),
        (
            "909000222",
            "guarantee_policy_reuse:segurosmundial:100777888:CREUSE-2",
            "high",
            "same_policy_two_contracts_different_supplier_buyer",
            "CREUSE-2",
            "SEGUROS MUNDIAL",
            "100777888",
            "Expirada",
            700_000_000.0,
            2,
            2,
            2,
            1,
            1,
            1_300_000_000.0,
            700_000_000.0,
            True,
            True,
            True,
            True,
            True,
            True,
            False,
            "https://secop.example/CREUSE-2",
            "secop_guarantees:CREUSE-2:100777888:1",
            "https://secop.example/CREUSE-1",
            "secop_guarantees:CREUSE-1:100777888:1",
        ),
    ]
    assert budget_chain_rows == [
        (
            "907000113",
            "CPAY-1",
            "critical",
            5,
            False,
            False,
            False,
            True,
            True,
            True,
            True,
            False,
            True,
            True,
            1_000_000_000.0,
            0.0,
            0.0,
            0.0,
            "https://secop.example/CPAY-1",
            "secop_cdp_requests:CPAY-1:CDP-CPAY-1",
            "secop_budget_commitments:CPAY-1:COMP-CPAY-1",
            "secop_budget_items:CPAY-1:RUBRO-CPAY-1",
            "signal_feature_procurement_guarantee_advance_execution_chain:CPAY-1",
        )
    ]
    assert invoice_budget_rows == [
        (
            "907000113",
            "CPAY-1",
            "critical",
            "confirmed_invoice_value_exceeds_contract",
            4,
            True,
            True,
            True,
            True,
            True,
            True,
            1_000_000_000.0,
            1_400_000_000.0,
            1_400_000_000.0,
            1_400_000_000.0,
            400_000_000.0,
            "https://secop.example/CPAY-1",
            "secop_invoices:CPAY-1:INV-CPAY-1",
            (
                "signal_feature_procurement_budget_chain_reconciliation_review_only:"
                "CPAY-1"
            ),
            "signal_feature_procurement_guarantee_advance_execution_chain:CPAY-1",
        )
    ]
    assert payment_plan_reconciliation_rows == [
        (
            "907000113",
            "CPAY-1",
            "critical",
            "actual_payment_value_exceeds_contract",
            5,
            True,
            True,
            True,
            True,
            False,
            True,
            True,
            True,
            True,
            1_000_000_000.0,
            1_500_000_000.0,
            1_500_000_000.0,
            500_000_000.0,
            1_500_000_000.0,
            1_500_000_000.0,
            1_500_000_000.0,
            "https://secop.example/CPAY-1",
            "secop_payment_plans:CPAY-1:PAY-CPAY-1",
            (
                "signal_feature_procurement_budget_chain_reconciliation_review_only:"
                "CPAY-1"
            ),
            "signal_feature_procurement_guarantee_advance_execution_chain:CPAY-1",
            (
                "signal_feature_procurement_invoice_budget_reconciliation_review_only:"
                "CPAY-1"
            ),
        )
    ]
    assert health_pae_rows == [
        (
            "907000113",
            "CPAY-1",
            "critical",
            "health_provider_and_pae",
            True,
            True,
            "Instituciones Prestadoras de Servicios de Salud - IPS",
            4,
            2,
            ["execution_failure", "payment_budget"],
            [
                "procurement_budget_chain_reconciliation_review_only",
                "procurement_guarantee_advance_execution_chain",
                "procurement_invoice_budget_reconciliation_review_only",
                "procurement_payment_plan_reconciliation_review_only",
            ],
            "https://secop.example/CPAY-1",
            "health_providers:REPS-907000113-SEDE",
            "signal_feature_procurement_budget_chain_reconciliation_review_only:CPAY-1",
        )
    ]
    assert pae_beneficiary_rows == [
        (
            "907000113",
            "pae_beneficiary_territory:CPAY-1",
            "critical",
            "health_provider_and_pae",
            "CPAY-1",
            2026,
            True,
            12800.0,
            800.0,
            800.0,
            800.0,
            2,
            4,
            2,
            "signal_feature_health_pae_service_delivery_gap_review_only:CPAY-1",
            "https://secop.example/CPAY-1",
            "pae_indicators:11001:2026:Indígenas:2",
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
    assert modification_ladder_rows == [
        (
            "908000113",
            "CLADDER-1",
            "high",
            600_000_000.0,
            3,
            0.0,
            210,
            4,
            0,
            False,
            True,
            True,
            True,
            True,
            "https://secop.example/CLADDER-1",
            "secop_contract_modifications:MOD-LADDER-2",
            "secop_contract_modifications:MOD-LADDER-1",
            "secop_contract_modifications:MOD-LADDER-3",
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
    assert bpin_project_rows == [
        (
            "202612345678901",
            "bpin:202612345678901",
            "medium",
            "2026",
            2,
            2,
            1,
            120_000_000_000.0,
            "secop_process_bpin:202612345678901:BPIN-C-1",
            "https://secop.example/BPIN-C-1",
        )
    ]
    assert regalias_project_rows == [
        (
            "202612345678901",
            "sgr_bpin:202612345678901",
            "medium",
            "Proyecto SGR con contratacion validada",
            2,
            120_000_000_000.0,
            1,
            60_000_000_000.0,
            60_000_000_000.0,
            "sgr_projects:202612345678901",
            "sgr_expense_execution:202612345678901:20260901:700001:2.3.2.02",
            "secop_process_bpin:202612345678901:BPIN-C-1",
            "https://secop.example/BPIN-C-1",
        )
    ]
    assert sgr_ocad_rows == [
        (
            "202612345678901",
            "sgr_ocad_capacity:202612345678901",
            "critical",
            "Proyecto SGR con contratacion validada",
            130_000_000_000.0,
            120_000_000_000.0,
            52_000_000_000.0,
            42.0,
            38.0,
            3,
            True,
            True,
            True,
            False,
            False,
            "sgr_projects:202612345678901",
            "sgr_expense_execution:202612345678901:20260901:700001:2.3.2.02",
            "secop_process_bpin:202612345678901:BPIN-C-1",
            "https://secop.example/BPIN-C-1",
        )
    ]
    assert dnp_sgr_rows == [
        (
            "202612345678901",
            "dnp_sgr_beneficiary_delivery:202612345678901",
            "critical",
            1,
            1,
            12000.0,
            12000.0,
            3,
            [
                "dnp_beneficiary_context",
                "vulnerable_population_context",
                "high_demographic_observation_context",
            ],
            "dnp_project_executors:202612345678901:700001",
            (
                "dnp_project_beneficiary_characterization:"
                "202612345678901:Personas con discapacidad"
            ),
        )
    ]
    assert compound_risk_rows == [
        (
            "Company",
            "900123456",
            "critical",
            6,
            3,
            7,
            ["campaign_finance", "execution_failure", "sanctions"],
            [
                "cuentas_claras_donor_ineligibility_review",
                "cuentas_claras_donor_supplier_overlap",
                "fiscal_procurement_chronology_review_only",
                "procurement_contract_execution_delay",
                "procurement_large_modifications",
                "procurement_sanctioned_supplier_awarded",
            ],
            (
                "signal_feature_fiscal_procurement_chronology_review_only:"
                "fiscal_procurement:fiscal_responsibility:RF-2020-1:1:900123456"
            ),
            (
                "signal_feature_procurement_large_modifications:C-1"
            ),
        ),
        (
            "Company",
            "902000111",
            "high",
            3,
            3,
            3,
            [
                "conflict_interest",
                "corporate_capacity",
                "procurement_competition",
            ],
            [
                "procurement_cross_source_identity_inconsistency",
                "procurement_politically_exposed_position_supplier_overlap",
                "procurement_single_bidder_high_value",
            ],
            "signal_feature_procurement_single_bidder_high_value:PV-1",
            (
                "signal_feature_procurement_politically_exposed_position_supplier_overlap:"
                "sensitive_position:123456789:INST-1:902000111"
            ),
        ),
            (
                "Company",
                "907000113",
                "critical",
                7,
                3,
                7,
                ["corporate_capacity", "execution_failure", "service_delivery"],
                [
                    "health_pae_service_delivery_gap_review_only",
                    "pae_beneficiary_territory_delivery_gap_review_only",
                    "procurement_guarantee_advance_execution_chain",
                    "procurement_invoice_budget_reconciliation_review_only",
                    "procurement_payment_plan_anomalies",
                    "procurement_payment_plan_reconciliation_review_only",
                    "rues_supplier_capacity_status_review_only",
                ],
                "signal_feature_health_pae_service_delivery_gap_review_only:CPAY-1",
                (
                    "signal_feature_pae_beneficiary_territory_delivery_gap_review_only:"
                    "pae_beneficiary_territory:CPAY-1"
            ),
        ),
    ]
    assert secop_i_legacy_rows == [
        (
            "907000113",
            "secop_i_legacy_current_risk:907000113",
            "critical",
            "critical",
            7,
            3,
            2,
            2,
            65_000_000_000.0,
            6_000_000_000.0,
            1,
            2,
            (
                "signal_feature_cross_signal_compound_risk_review_only:"
                "cross_signal:company:907000113"
            ),
            True,
        )
    ]
    assert secop_i_legacy_representative_rows == [
        (
            "907000113",
            "secop_i_legacy_representative_current_risk:333333333:907000113",
            "high",
            "critical",
            7,
            3,
            "333333333",
            "333333333",
            "333333333",
            1,
            1,
            1,
            1,
            6_000_000_000.0,
            600_000_000.0,
            1,
            1,
            (
                "signal_feature_cross_signal_compound_risk_review_only:"
                "cross_signal:company:907000113"
            ),
            True,
            True,
        )
    ]
    assert declaration_company_bridge_rows == [
        (
            "907000113",
            "declaration_company_bridge:333333333:907000113",
            "critical",
            "critical",
            7,
            3,
            "333333333",
            "333333333",
            1,
            1,
            True,
            (
                "signal_feature_cross_signal_compound_risk_review_only:"
                "cross_signal:company:907000113"
            ),
            True,
            True,
        )
    ]
    assert interadmin_rows == [
        (
            "907000113",
            "interadmin_chain:IA-1:907000113",
            "high",
            "Proveedor Pago Anomalo SAS",
            "IA-1",
            12_000_000_000.0,
            6,
            3,
            ["corporate_capacity", "execution_failure", "service_delivery"],
            [
                "health_pae_service_delivery_gap_review_only",
                "procurement_guarantee_advance_execution_chain",
                "procurement_invoice_budget_reconciliation_review_only",
                "procurement_payment_plan_anomalies",
                "procurement_payment_plan_reconciliation_review_only",
                "rues_supplier_capacity_status_review_only",
            ],
            "https://secop.example/IA-1",
            "signal_feature_procurement_budget_chain_reconciliation_review_only:CPAY-1",
        )
    ]
    assert bpin_priority_rows == [
        (
            "202699990000001",
            "bpin_priority_work:202699990000001",
            "medium",
            "NARINO",
            "TUMACO",
            3,
            3,
            30_000_000_000.0,
            "secop_process_bpin:202699990000001:BPIN-PRIO-1",
            "https://secop-integrado.example/BPIN-PRIO-1",
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
    assert conflict_disclosure_rows == [
        (
            "1001234567",
            "disclosure:FORM-1:1001234567",
            "medium",
            "FORM-1",
            1,
            4,
            2,
            2_095_000_000.0,
            "conflict_disclosures:FORM-1",
            "https://secop.example/CPER-1",
        )
    ]
    assert role_supplier_rows == [
        (
            "1001234567",
            "role_supplier:800111222:1001234567:supervisor_doc_number",
            "high",
            "supervisor_doc_number",
            "800111222",
            1,
            2,
            845000000.0,
            "secop_ii_contracts:role:C-1:supervisor_doc_number",
            "https://secop.example/CPER-1",
        )
    ]
    assert declaration_supplier_rows == [
        (
            "1001234567",
            "declaration_supplier:1001234567:800111222",
            "high",
            "800111222",
            2,
            845000000.0,
            1,
            0,
            "conflict_disclosures:FORM-1",
            "https://secop.example/CPER-1",
        ),
        (
            "1001234567",
            "declaration_supplier:1001234567:800111333",
            "high",
            "800111333",
            2,
            1250000000.0,
            1,
            0,
            "conflict_disclosures:FORM-1",
            "https://secop.example/CPER-2",
        ),
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
    assert cuentas_ineligibility_rows == [
        (
            "900123456",
            "donor_contract:2019:mayor:700111222:900123456:800111222",
            "medium",
            "mayor",
            "mayor_municipality",
            "missing_campaign_cap",
            "missing_election_result",
            "inferred_2019_local_term",
            5_000_000.0,
            1,
            125_000_000.0,
            "800111222",
            "cuentas_claras_income_2019:VOUCHER-1",
            "https://secop.example/C-1",
        )
    ]
    assert pida_chain_rows == [
        (
            "BOGOTA:BOGOTA",
            "pida5_pida27_pida4_chain:BOGOTA:BOGOTA",
            "medium",
            "BOGOTA",
            "BOGOTA",
            1,
            1,
            150_000_000.0,
            "secop_sanctions:ACT-SAN-1",
            "https://secop-integrado.example/INT-1",
        )
    ]
    assert pida_full_rows == [
        (
            "BOGOTA:BOGOTA",
            "pida_full30_meta:BOGOTA:BOGOTA",
            "medium",
            "BOGOTA",
            "BOGOTA",
            504,
            12,
            5_170_000_000_000.0,
            504,
            "https://secop-integrado.example/PIDA-FULL-school_feeding-000",
        )
    ]
    assert tvec_capture_rows == [
        (
            "906000111",
            "tvec_supplier:906000111",
            "medium",
            100,
            50,
            1_010_000_000.0,
            50,
            5_000_000_000.0,
            "tvec_orders_consolidated:TVEC-0",
            "https://secop.example/CN-0-0",
        )
    ]
    assert tvec_price_rows == [
        (
            "910000002",
            "tvec_price:termometrodigitalqa:unidad:q_001:2026:TVEC-PRICE-19:910000002",
            "high",
            "TVEC-PRICE-19",
            "820000004",
            "termometrodigitalqa",
            "unidad",
            "q_001",
            "2026",
            500.0,
            500.0,
            20,
            3,
            5,
            100.0,
            300.0,
            310.0,
            "tvec_orders_consolidated:TVEC-PRICE-19:termometrodigitalqa",
            "tvec_orders_consolidated:TVEC-PRICE-0:termometrodigitalqa",
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
            51,
            5_800_000_000.0,
            "company_registry_c82u:row-c82u-shared-1",
            "company_registry_c82u:row-c82u-shared-2",
            "https://secop.example/CREL-1",
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
    assert related_bidders_rows == [
        (
            "900765432",
            "related_bidders_process:PREL-1:900765432:901000111:222222222",
            "high",
            "PREL-1",
            "CREL-1",
            800_000_000.0,
            "901000111",
            "Proveedor Recurrente SAS",
            790_000_000.0,
            2,
            1,
            "222222222",
            True,
            False,
            False,
            True,
            "https://secop.example/CREL-1",
            "secop_offers:PREL-1:OFFER-ID-RELATED-LOSER",
            (
                "signal_feature_procurement_related_companies_shared_officer:"
                "officer_cluster:222222222:900765432"
            ),
            (
                "signal_feature_procurement_related_companies_shared_officer:"
                "officer_cluster:222222222:901000111"
            ),
        )
    ]
    assert shared_representative_same_buyer_rows == [
        (
            "910111222",
            (
                "shared_representative_same_buyer:"
                "444444444:800999888:2026:910111222"
            ),
            "high",
            "800999888",
            2026,
            "444444444",
            2,
            6,
            5_400_000_000.0,
            6,
            4,
            3_600_000_000.0,
            "company_registry_c82u:row-c82u-samebuyer-1",
            True,
            True,
            True,
            True,
        ),
        (
            "910111333",
            (
                "shared_representative_same_buyer:"
                "444444444:800999888:2026:910111333"
            ),
            "high",
            "800999888",
            2026,
            "444444444",
            2,
            6,
            5_400_000_000.0,
            6,
            2,
            1_800_000_000.0,
            "company_registry_c82u:row-c82u-samebuyer-1",
            True,
            True,
            True,
            True,
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
    assert rues_capacity_rows == [
        (
            "907000113",
            "rues_capacity:907000113",
            "high",
            "Proveedor Pago Anomalo SAS",
            "Proveedor Pago Anomalo SAS",
            1,
            1,
            1_000_000_000.0,
            1_000_000_000.0,
            False,
            True,
            "2026-05-01",
            2024,
            "CANCELADA",
            True,
            False,
            False,
            False,
            1,
            "company_registry_c82u:row-c82u-capacity-1",
            "https://secop.example/CPAY-1",
        )
    ]
    assert company_rows == [
        ("9001234568", ["paco_sanctions", "secop_ii_contracts"]),
        ("9007654326", ["secop_ii_contracts"]),
        ("9010001118", ["secop_ii_contracts"]),
        ("9020001111", ["secop_ii_contracts"]),
        ("9033331114", ["secop_ii_contracts"]),
        ("9033332223", ["secop_ii_contracts"]),
        ("9051239993", ["secop_ii_contracts"]),
        ("9060001116", ["secop_ii_contracts"]),
        ("9070001134", ["secop_ii_contracts"]),
        ("9080001138", ["secop_ii_contracts"]),
        ("9101112226", ["secop_ii_contracts"]),
        ("9101113335", ["secop_ii_contracts"]),
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
