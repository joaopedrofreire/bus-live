import requests

def get_gps_sppo(data_inicial, data_final):
    """
    Deve retornar uma LISTA de dicts no formato:
    {
      'ordem': 'D12123',
      'latitude': '-22,8894',
      'longitude': '-43,58953',
      ...
    }
    """

    # EXEMPLO (mock). Substitua pela API real
    response = requests.get(
        "https://api.exemplo.com/gps",
        params={
            "dataInicial": data_inicial,
            "dataFinal": data_final
        },
        timeout=10
    )

    response.raise_for_status()
    return response.json()
