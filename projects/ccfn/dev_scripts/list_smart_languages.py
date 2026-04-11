"""
Utility script to list available languages for a SMART Connect conservation area.

Usage:
    python projects/ccfn/list_smart_languages.py

Reads the CCFN-SMART connection from environment variables and prints all
language UUIDs and names configured for the conservation area. Copy the UUID
for the language you want and set it as CCFN_SMART_LANGUAGE_UUID.
"""
import os

from ecoscope.io import SmartIO

SERVER   = os.environ["CCFN_SMART_SERVER"].rstrip("/") + "/"
USERNAME = os.environ["CCFN_SMART_USERNAME"]
PASSWORD = os.environ["CCFN_SMART_PASSWORD"]
CA_UUID  = os.environ["CCFN_SMART_CA_UUID"]

if __name__ == "__main__":
    client = SmartIO(urlBase=SERVER, username=USERNAME, password=PASSWORD)

    try:
        df = client.query_data(f"ca/{CA_UUID}/language/")
        if df.empty:
            print("No languages returned. Check the CA UUID or your connection.")
        else:
            print(f"\nLanguages for CA {CA_UUID}:\n")
            for _, row in df.iterrows():
                uuid_col = next((c for c in df.columns if "uuid" in c.lower()), None)
                name_col = next((c for c in df.columns if "name" in c.lower()), None)
                uuid_val = row[uuid_col] if uuid_col else "?"
                name_val = row[name_col] if name_col else str(row.to_dict())
                print(f"  {name_val:<30}  UUID: {uuid_val}")
            print()
            print("Set your chosen UUID with:")
            print("  set CCFN_SMART_LANGUAGE_UUID=<uuid>")
    except Exception as e:
        print(f"Error querying languages: {e}")
        print("\nAlternatively, find the language UUID in the SMART Connect web UI:")
        print("  Conservation Area Settings → Languages tab")
