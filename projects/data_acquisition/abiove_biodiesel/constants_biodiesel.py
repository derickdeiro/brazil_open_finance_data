ITF = 10227
FAMCOML = 'Geral'
INTERVALO = 4
SOURCE_NAME= 'abiove_biodiesel'

SHEETS_INFOS = [{'sheet_name': 'producao_m3_total', 
                 'serie_name': 'Produção mensal de biodiesel (m³)'},
                {'sheet_name': 'producao_m3_regiao',
                 'serie_name': 'Produção de biodiesel por estado, região e agregado nacional (m³) - Acumulado no Ano'},
                {'sheet_name': 'entrega_venda_m3', 
                 'serie_name': 'Entrega mensal de biodiesel nos leilões ANP e vendas diretas (m³)'},
                {'sheet_name': 'materia-prima_anual', 
                 'serie_name': ['Produção de biodiesel por matéria-prima (m³) - Acumulado no Ano',
                                'Produção de biodiesel por matéria-prima (%) - Acumulado no Ano']},
                {'sheet_name': 'vendas_importacao_dieselB', 
                 'serie_name': ['Venda mensal, pelas distribuidoras, de diesel B (m³)',
                                'Importações de diesel A (US$ FOB)',
                                'Importações de diesel A (m³)',
                                'Participação das importações nas vendas de diesel B (%)',
                                'Participação das importações nas vendas de diesel A (%)']},
                ]

MONTH_DICT = {
'JAN': '01',
'FEV': '02',
'MAR': '03',
'ABR': '04',
'MAI': '05',
'JUN': '06',
'JUL': '07',
'AGO': '08',
'SET': '09',
'OUT': '10',
'NOV': '11',
'DEZ': '12'
}
