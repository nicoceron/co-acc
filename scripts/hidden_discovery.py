import urllib.request
import json
import time
from collections import Counter

def fetch_full_catalog():
    datasets = []
    limit = 1000
    offset = 0
    # Trying to fetch up to 40,000 datasets (typical for large Socrata portals)
    for _ in range(40):
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
                # print(f"Fetched {len(datasets)}...")
        except Exception as e:
            # print(f"Error at offset {offset}: {e}")
            break
        time.sleep(0.5)
    return datasets

def deep_analysis(datasets):
    agencies = Counter()
    tags = Counter()
    categories = Counter()
    
    # "Control" Agencies or interesting entities
    control_keywords = [
        'contraloria', 'procuraduria', 'fiscalia', 'superintendencia', 
        'transparencia', 'anticorrupcion', 'auditoria', 'dian', 'dnp'
    ]
    
    interesting_datasets = []
    
    for item in datasets:
        resource = item.get('resource', {})
        classification = item.get('classification', {})
        
        agency = item.get('creator', {}).get('display_name', 'Unknown')
        agencies[agency] += 1
        
        res_tags = classification.get('domain_tags', [])
        for t in res_tags:
            tags[t.lower()] += 1
            
        cat = classification.get('domain_category', 'None')
        categories[cat] += 1
        
        # Broader matching: Agency name or Tag contains control keywords
        text_to_check = (agency + " " + " ".join(res_tags) + " " + cat + " " + resource.get('name', '')).lower()
        
        if any(kw in text_to_check for kw in control_keywords):
            interesting_datasets.append({
                'id': resource.get('id'),
                'name': resource.get('name'),
                'agency': agency,
                'tags': res_tags,
                'updated': resource.get('updatedAt', '')
            })

    print("\n=== TOP AGENCIES (Count) ===")
    for a, count in agencies.most_common(20):
        print(f"{count} | {a}")

    print("\n=== TOP RELEVANT TAGS ===")
    for t, count in tags.most_common(100):
        if any(kw in t for kw in control_keywords):
            print(f"{count} | {t}")

    print("\n=== HIDDEN GEMS (Interesting Agencies/Tags with non-obvious names) ===")
    # Filter for things that don't have obvious names but are from control agencies
    for d in interesting_datasets[:50]:
        name_lower = d['name'].lower()
        # If the name is vague but agency is strong
        if len(name_lower.split()) < 4:
            print(f"ID: {d['id']} | NAME: {d['name']} | AGENCY: {d['agency']}")

if __name__ == "__main__":
    datasets = fetch_full_catalog()
    deep_analysis(datasets)
