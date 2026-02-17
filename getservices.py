import requests
from dotenv import load_dotenv
import os

load_dotenv()

server_url = os.getenv("SERVERURL")
agsuser = os.getenv("SERVERUSER")
agspassword = os.getenv("SERVERPASS")


def get_all_map_services(server_url, agsuser, agspassword):
    token_params = {
        "username": agsuser,
        "password": agspassword,
        "client": "requestip",
        "f": "json"
    }
    token_response = requests.post(f"{server_url}/arcgis/tokens/generateToken", data=token_params)
    token = token_response.json().get("token")
    folders_response = requests.get(f"{server_url}/arcgis/admin/services", params={"token": token, "f": "json"})
    folders = folders_response.json().get("folders", [])
    all_services = []
    for folder in folders:
        if folder in ["System", "TEST", "Utilities"]:
            continue
        services_response = requests.get(f"{server_url}/arcgis/admin/services/{folder}", params={"token": token, "f": "json"})
        services = services_response.json().get("services", [])
        for s in services:
            #print(s)
            all_services.append(f"{folder}.{s['serviceName']}")
    return all_services

def main():
    from dotenv import load_dotenv
    import os
    
    load_dotenv()
    
    server_url = os.getenv("SERVERURL")
    agsuser = os.getenv("SERVERUSER")
    agspassword = os.getenv("SERVERPASS")
    
    services = get_all_map_services(server_url, agsuser, agspassword)
    return services

if __name__ == "__main__":
    #services = main()
    #print(services)
    import time
    print("Hey")
    for i in range(10):
        time.sleep(0.3)
        print(f'\rAnalyzing file {i}', end="")
    print("\n")
    print("\nHo")