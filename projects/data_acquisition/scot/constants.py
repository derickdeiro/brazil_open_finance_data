import numpy as np
import re

ITF = 10095
FAMCOML = 'Geral'
INTERVALO = 1
SOURCE_NAME='scot'

ASSETS = ['boigordo', 'vacagorda', 'milho', 'soja', 'reposicao']

estados_brasil = {
    "Acre": "AC",
    "Alagoas": "AL",
    "Amapá": "AP",
    "Amazonas": "AM",
    "Bahia": "BA",
    "Ceará": "CE",
    "Distrito Federal": "DF",
    "Espírito Santo": "ES",
    "Goiás": "GO",
    "Maranhão": "MA",
    "Mato Grosso": "MT",
    "Mato Grosso do Sul": "MS",
    "Minas Gerais": "MG",
    "Pará": "PA",
    "Paraíba": "PB",
    "Paraná": "PR",
    "Pernambuco": "PE",
    "Piauí": "PI",
    "Rio de Janeiro": "RJ",
    "Rio Grande do Norte": "RN",
    "Rio Grande do Sul": "RS",
    "Rondônia": "RO",
    "Roraima": "RR",
    "Santa Catarina": "SC",
    "São Paulo": "SP",
    "Sergipe": "SE",
    "Tocantins": "TO"
}

regioes_uf = {
    "AC": "Região Norte",
    "AP": "Região Norte",
    "AM": "Região Norte",
    "PA": "Região Norte",
    "RO": "Região Norte",
    "RR": "Região Norte",
    "TO": "Região Norte",

    "AL": "Região Nordeste",
    "BA": "Região Nordeste",
    "CE": "Região Nordeste",
    "MA": "Região Nordeste",
    "PB": "Região Nordeste",
    "PE": "Região Nordeste",
    "PI": "Região Nordeste",
    "RN": "Região Nordeste",
    "SE": "Região Nordeste",

    "DF": "Região Centro-Oeste",
    "GO": "Região Centro-Oeste",
    "MT": "Região Centro-Oeste",
    "MS": "Região Centro-Oeste",

    "ES": "Região Sudeste",
    "MG": "Região Sudeste",
    "RJ": "Região Sudeste",
    "SP": "Região Sudeste",

    "PR": "Região Sul",
    "RS": "Região Sul",
    "SC": "Região Sul"
}

regioes_dict = {
    "Norte": "Região Norte",
    "Noroeste": "Região Norte",
    "Nordeste": "Região Nordeste",
    "Oeste": "Região Oeste",
    "Sudoeste": "Região Oeste",
    "Sudeste": "Região Sudeste",
    "Sul": "Região Sul",
    "Reg. Sul": "Região Sul",
    }

cidades_boi_gordo = {
    'B.Horizonte': 'Belo Horizonte',
    'C. Grande': 'Campo Grande',
    '': np.nan
    }

reposicao_dict = {
    'Macho Nelore': 'Nelore',
    'Macho Mestiço': 'Mestiço',
    'Fêmea Nelore': 'Nelore',
    'Fêmea Mestiça': 'Mestiço',
    'Bezerro': 'Bezerro 12 meses',
    'Bezerra': 'Bezerra 12 meses',
    'Bza Desmama': 'Bezerra desmama',
    'Bzo Desmama': 'Bezerro desmama',
    'Vaca Boiadeira': 'Vaca magra',
    'Boi Magro': 'Boi magro'
    }
