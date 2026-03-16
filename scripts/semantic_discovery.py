import urllib.request
import json
import time

# High-signal column/variable names (Colombian Spanish)
HIGH_SIGNAL_FIELDS = {
    'identity': ['cedula', 'nit', 'cc', 'documento_identidad', 'identificacion', 'nuip', 'pasaporte'],
    'finance': ['valor_contrato', 'valor_total', 'monto_asignado', 'valor_pagado', 'valor_adicion', 'presupuesto'],
    'entities': ['razon_social', 'nombre_contratista', 'nombre_representante', 'sociedad', 'consorcio', 'union_temporal'],
    'control': ['hallazgo', 'observacion', 'irregularidad', 'alerta', 'incumplimiento', 'sancion', 'multa'],
    'infrastructure': ['avance_fisico', 'avance_financiero', 'fecha_terminacion', 'prorroga', 'suspension', 'obras_inconclusas']
}

CONTROL_AGENCIES = [
    'Contraloría', 'Procuraduría', 'Fiscalía', 'Superintendencia', 'DNP', 'DIAN', 'Auditoría', 'Transparencia'
]

def search_by_term(term):
    """Search Socrata catalog for a specific term (indices include columns/metadata)."""
    url = f"https://api.us.socrata.com/api/catalog/v1?domains=www.datos.gov.co&search_context=www.datos.gov.co&only=datasets&q={urllib.parse.quote(term)}&limit=20"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as response:
            return json.loads(response.read().decode()).get('results', [])
    except:
        return []

def get_columns(dataset_id):
    """Fetch the actual column names (variable names) for a dataset."""
    url = f"https://www.datos.gov.co/api/views/{dataset_id}.json"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())
            cols = data.get('columns', [])
            return [c.get('fieldName', '').lower() for c in cols]
    except:
        return []

def deep_semantic_discovery():
    found_gems = {}
    
    # Strategy 1: Search for datasets by field names directly
    search_terms = ['nit', 'cedula', 'hallazgo fiscal', 'nomina', 'beneficiario']
    
    for term in search_terms:
        # print(f"Searching term: {term}")
        results = search_by_term(term)
        for res in results:
            ds_id = res['resource']['id']
            if ds_id in found_gems: continue
            
            ds_name = res['resource']['name']
            agency = res.get('creator', {}).get('display_name', 'Unknown')
            
            # Strategy 2: Deep Inspect Columns
            cols = get_columns(ds_id)
            
            matches = []
            for category, fields in HIGH_SIGNAL_FIELDS.items():
                if any(f in " ".join(cols) for f in fields):
                    matches.append(category)
            
            if matches:
                found_gems[ds_id] = {
                    'name': ds_name,
                    'agency': agency,
                    'matches': matches,
                    'columns': cols[:10] # Top 10 columns for preview
                }
        time.sleep(1)

    print("\n=== DEEP SEMANTIC DISCOVERY RESULTS (Potential Corruption Data Gems) ===")
    for ds_id, info in found_gems.items():
        # Heuristic: If it has identity + finance/control but a vague name, it's a "Gem"
        is_vague = len(info['name'].split()) < 4 or any(w in info['name'].lower() for w in ['base', 'datos', 'registro', 'archivo'])
        agency_prio = any(a.lower() in info['agency'].lower() for a in CONTROL_AGENCIES)
        
        if is_vague or agency_prio:
            print(f"\n💎 ID: {ds_id} | NAME: {info['name']}")
            print(f"   Agency: {info['agency']}")
            print(f"   Signals: {', '.join(info['matches'])}")
            print(f"   Fields: {', '.join(info['columns'])}")

if __name__ == "__main__":
    import urllib.parse
    deep_semantic_discovery()
