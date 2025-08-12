ITF = 10235
FAMCOML = 'Geral'
INTERVALO = 1

SOURCE_NAME = 'cvm_funds'

attrib_classe = ['c312', 'c4145', 'c264', 'c265', 'c4157', 'c266', 'c4146', 'c277', 'c289', 'c290', 'c291', 'c5107', 'c292', 'c293', 'c294', 'c295', 'c4152', 'c296', 'c297', 'c298', 'c299', 'c307', 'c308', 'c309', 'c310', 'c311', 'a618']
columns_classe = ['ID_Registro_Fundo', 'ID_Registro_Classe', 'CNPJ_Classe', 'Codigo_CVM', 'Data_Registro', 'Data_Constituicao', 'Data_Inicio', 'Tipo_Classe', 'Situacao', 'Classificacao', 'Indicador_Desempenho', 'Classe_Cotas', 'Classificacao_Anbima', 'Tributacao_Longo_Prazo', 'Entidade_Investimento', 'Permitido_Aplicacao_CemPorCento_Exterior', 'Classe_ESG', 'Forma_Condominio', 'Exclusivo', 'Publico_Alvo', 'CNPJ_Auditor', 'Auditor', 'CNPJ_Custodiante', 'Custodiante', 'CNPJ_Controlador', 'Controlador', 'Denominacao_Social']

DICT_CLASSE = dict(zip(columns_classe, attrib_classe))

attrib_fundo = ['c312', 'c313', 'c314', 'c4142', 'c315', 'c316', 'c317', 'c318', 'c319', 'c320', 'c321', 'c322', 'c323', 'c324', 'c5113', 'c5112', 'c4137', 'c5116', 'c5117', 'a642']
columns_fundo = ['ID_Registro_Fundo', 'CNPJ_Fundo', 'Codigo_CVM', 'Data_Registro', 'Data_Constituicao', 'Tipo_Fundo', 'Data_Cancelamento', 'Situacao', 'Data_Inicio_Situacao', 'Data_Adaptacao_RCVM175', 'Data_Inicio_Exercicio_Social', 'Data_Fim_Exercicio_Social', 'Data_Patrimonio_Liquido', 'Diretor', 'CNPJ_Administrador', 'Administrador', 'Tipo_Pessoa_Gestor', 'CPF_CNPJ_Gestor', 'Gestor', 'Denominacao_Social']

DICT_FUNDO = dict(zip(columns_fundo, attrib_fundo))

attrib_subclasse = ['c4145', 'c4234', 'c325', 'c326', 'c4236', 'c327', 'c328', 'c329', 'c330', 'a643']
columns_subclasse = ['ID_Registro_Classe', 'ID_Subclasse', 'Codigo_CVM', 'Data_Constituicao', 'Data_Inicio', 'Situacao', 'Forma_Condominio', 'Exclusivo', 'Publico_Alvo', 'Denominacao_Social']

DICT_SUBCLASSE = dict(zip(columns_subclasse, attrib_subclasse))

attrib_diario = ['DATASER', 'a68', 'a8', 'a20', 'a314', 'a279', 'a281']
columns_diario = ['DT_COMPTC', 'VL_TOTAL', 'VL_QUOTA', 'VL_PATRIM_LIQ', 'CAPTC_DIA', 'RESG_DIA', 'NR_COTST']

DICT_DIARIO = dict(zip(columns_diario, attrib_diario))
