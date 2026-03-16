import urllib.request
import json
import time

def fetch_catalog():
    datasets = []
    limit = 1000
    offset = 0
    while True:
        url = f"https://api.us.socrata.com/api/catalog/v1?domains=www.datos.gov.co&search_context=www.datos.gov.co&limit={limit}&offset={offset}"
        try:
            req = urllib.request.Request(url)
            with urllib.request.urlopen(req) as response:
                data = json.loads(response.read().decode())
                results = data.get('results', [])
                if not results:
                    break
                datasets.extend(results)
                offset += limit
                print(f"Fetched {len(datasets)} datasets...")
        except Exception as e:
            print(f"Error fetching: {e}")
            break
        time.sleep(1)
    return datasets

def analyze_datasets(datasets):
    keywords = {
        'sanctions': ['sancion', 'sanción', 'multa', 'penal', 'disciplinari', 'inhabilitad', 'responsabilidad fiscal', 'boletín'],
        'assets_income': ['declaracion', 'declaración', 'bienes', 'rentas', 'conflicto de inter', 'ley 2013'],
        'public_servants': ['funcionario', 'servidor publico', 'servidor público', 'nomina', 'nómina', 'salario', 'prestacion de servicios'],
        'subsidies': ['subsidio', 'beneficiario', 'familias en accion', 'ingreso solidario', 'colombia mayor'],
        'infrastructure': ['obra', 'infraestructura', 'elefante blanco', 'inconclusa', 'regalia', 'regalía', 'sgr', 'proyecto'],
        'health': ['adres', 'eps', 'ips', 'recobro', 'salud', 'medicamento'],
        'education_pae': ['pae', 'alimentacion escolar', 'alimentación escolar'],
        'elections': ['cuentas claras', 'campaña', 'eleccion', 'elección', 'candidato', 'donante', 'financiaci'],
        'land': ['tierra', 'predio', 'catastro', 'restitucion', 'ant', 'incoder'],
        'corporate': ['superintendencia', 'sociedade', 'beneficiario final', 'accionista', 'junta directiva']
    }

    categorized = {k: [] for k in keywords}
    
    for item in datasets:
        resource = item.get('resource', {})
        name = resource.get('name', '').lower()
        desc = resource.get('description', '').lower()
        text = name + " " + desc
        
        for cat, words in keywords.items():
            if any(w in text for w in words):
                categorized[cat].append({
                    'id': resource.get('id'),
                    'name': resource.get('name'),
                    'agency': item.get('creator', {}).get('display_name', 'Unknown'),
                    'updated': resource.get('updatedAt', ''),
                    'type': resource.get('type', '')
                })

    for cat, items in categorized.items():
        print(f"\n=== {cat.upper()} ({len(items)} found) ===")
        # Sort by updated date descending and take top 10 most recently updated for preview
        items.sort(key=lambda x: x['updated'], reverse=True)
        for i in items[:10]:
            print(f"- {i['id']} | {i['name']} | {i['updated'][:10]}")

if __name__ == "__main__":
    print("Starting deep discovery of datos.gov.co catalog...")
    datasets = fetch_catalog()
    print(f"Total datasets found: {len(datasets)}")
    analyze_datasets(datasets)
