import json
import os

def run_catalog_validation():
    catalog_path = "catalog/catalog.json"
    print(f"📋 Structural discovery validation for: {catalog_path}")
    
    if not os.path.exists(catalog_path):
        print(" Error: Structural mapping schema file missing.")
        return False
        
    with open(catalog_path, 'r') as f:
        metadata = json.load(f)
        
    required_layers = ["lake_historical_ledger", "warehouse_curated_snapshots"]
    for layer in required_layers:
        if layer not in metadata:
            print(f"Error: Discoverability registration path '{layer}' is unregistered.")
            return False
            
    print("Discovery boundaries are successfully validated.")
    return True

if __name__ == "__main__":
    run_catalog_validation()