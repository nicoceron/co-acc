import urllib.request
import json
import time

def search_socrata(query):
    url = f"https://api.us.socrata.com/api/catalog/v1?domains=www.datos.gov.co&search_context=www.datos.gov.co&only=datasets&q={urllib.parse.quote(query)}&limit=10"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as response:
            data = json.loads(response.read().decode())
            return data.get('results', [])
    except Exception as e:
        print(f"Error fetching {query}: {e}")
        return []

def main():
    queries = [
        "responsabilidad fiscal contraloria",
        "sanciones procuraduria",
        "sanciones disciplinarias",
        "elefante blanco",
        "obras inconclusas",
        "beneficiarios subsidio",
        "familias en accion",
        "cuentas claras",
        "donantes campaña",
        "cartel",
        "colusión",
        "inhabilitados",
        "registro unico de proponentes",
        "nomina funcionarios",
        "prestacion de servicios secop"
    ]
    
    for q in queries:
        print(f"\n=== SEARCH: '{q}' ===")
        results = search_socrata(q)
        for item in results:
            res = item['resource']
            print(f"- {res['id']} | {res['name']}")
            print(f"  Desc: {res.get('description', '')[:100]}...")
            
        time.sleep(1)

if __name__ == "__main__":
    import urllib.parse
    main()
