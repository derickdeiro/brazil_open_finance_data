import requests
import base64
from airflow.sdk import Variable

CLIENT_ID = Variable.get("anbima_client_id")

def get_token() -> str:

    # URL da API e dados de autenticação
    url_auth = "https://api.anbima.com.br/oauth/access-token"
    client_id = CLIENT_ID
    client_secret = Variable.get("anbima_client_secret")

    # Codifique as credenciais no formato Base64
    credentials = f"{client_id}:{client_secret}"
    base64_credentials = base64.b64encode(credentials.encode()).decode()

    # Cabeçalhos da solicitação
    headers_auth = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {base64_credentials}"
    }

    # Corpo da solicitação
    data = {
        "grant_type": "client_credentials"
    }

    # Faça a solicitação POST
    response = requests.post(url_auth, headers=headers_auth, json=data)

    # Verifique o status da resposta
    if response.status_code == 201:
        token = response.json()['access_token']
        return token
    else:
        # A solicitação falhou
        print(f"Falha na solicitação com código de status {response.status_code}")
        print("Resposta:")
        print(response.text)


def get_anbima_data(url_data: str) -> dict:
    token = get_token()

    headers_data = {"Content-Type": "application/json",
                'client_id': CLIENT_ID,
                'access_token': token}

    response_data = requests.get(url_data, headers=headers_data)

    return response_data