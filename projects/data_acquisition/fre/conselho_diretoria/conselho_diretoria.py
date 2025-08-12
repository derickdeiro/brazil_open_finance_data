import pandas as pd
from projects.data_acquisition.fre.fre_utils import (
    convert_format_date,
    normalize_json
)

from typing import Dict

def rename_key(d, old_key, new_key):
    d[new_key] = d[old_key]
    del d[old_key]


def transform_conselho_diretoria_cor_genero(fre_data: Dict) -> pd.DataFrame:
    """
    Transforma os dados de cor e gênero dos órgãos de administração do ECF.
    Args:
        fre_data (Dict): Dados do FRE.
    Returns:
        pd.DataFrame: DataFrame com os dados transformados.
    """
    #Region DescricaoCaracteristicasOrgaosAdmECF 
    descricao_cor = fre_data['AssembleiaGeralEAdm']['DescricaoCaracteristicasOrgaosAdmECF']['DescricaoCorRaca']['XmlFormularioReferenciaDadosFREFormularioAssembleiaGeralEAdmDescricaoCaracteristicasOrgaosAdmECFCorRaca']
    descricao_genero = fre_data['AssembleiaGeralEAdm']['DescricaoCaracteristicasOrgaosAdmECF']['DescricaoGenero']['XmlFormularioReferenciaDadosFREFormularioAssembleiaGeralEAdmDescricaoCaracteristicasOrgaosAdmECFGenero']

    df_cor = pd.DataFrame(descricao_cor)
    df_genero = pd.DataFrame(descricao_genero)

    df_transformed_descricao = pd.merge(df_cor, df_genero, how='left', on=['OrgaoAdministracao'])

    df_transformed_descricao = df_transformed_descricao.rename(columns={'Outros_x': 'Outros_Cor', 'Outros_y': 'Outros_Genero', 'PrefereNaoResponder_x': 'PrefereNaoResponder_Cor', 'PrefereNaoResponder_y': 'PrefereNaoResponder_Genero', 'NaoSeAplica_x': 'NaoSeAplica_Cor', 'NaoSeAplica_y': 'NaoSeAplica_Genero'})

    for coluna in df_transformed_descricao.columns:
        if coluna not in ['OrgaoAdministracao', 'NaoSeAplica_Cor', 'NaoSeAplica_Genero']:
            
            for i in range(len(df_transformed_descricao[coluna])):
                if df_transformed_descricao[coluna][i] == None:
                    df_transformed_descricao.loc[i, coluna] = 0
                df_transformed_descricao.loc[i, coluna] = int(df_transformed_descricao.loc[i, coluna])
    #endregion
    return df_transformed_descricao
    

def transform_conselho_diretoria_comp_conselho(fre_data: Dict) -> pd.DataFrame:
    """
    Transforma os dados de composição do conselho de administração e comitês do ECF.
    Args:
        fre_data (Dict): Dados do FRE.
    Returns:
        pd.DataFrame: DataFrame com os dados transformados.
    """
    #Region ComposicaoEExperienciasProfissionaisAdmECF
    if 'ComposicaoEExperienciasProfissionaisAdmECF' not in fre_data['AssembleiaGeralEAdm'].keys():
        df_transformed_composicao_conselho = pd.DataFrame()

    if 'ComposicaoEExperienciasProfissionaisAdmECF' in fre_data['AssembleiaGeralEAdm'].keys():
        composicao_conselho = fre_data['AssembleiaGeralEAdm']['ComposicaoEExperienciasProfissionaisAdmECF']
        df_normalized = normalize_json(composicao_conselho, 'Administrador_ExperienciaProfissional')

        df_transformed_composicao_conselho = pd.DataFrame(df_normalized)
        for coluna_data in df_transformed_composicao_conselho.columns:
            if 'Data' in coluna_data:
                df_transformed_composicao_conselho[coluna_data] = convert_format_date(df_transformed_composicao_conselho[coluna_data])
                

        for coluna_numero in df_transformed_composicao_conselho.columns:
            if coluna_numero in ['Administrador_OrgaoAdm', 'MembroDiretoriaCAdmCF_OrgaoAdm', 'MembroDiretoriaCAdmCF_EleitoPeloControlador']:
                df_transformed_composicao_conselho[coluna_numero] = df_transformed_composicao_conselho[coluna_numero].astype(int)
    #endregion
    return df_transformed_composicao_conselho


def transform_conselho_diretoria_comite(fre_data: Dict) -> pd.DataFrame:
    """
    Transforma os dados de composição do comitê do ECF.
    Args:
        fre_data (Dict): Dados do FRE.
    Returns:
        pd.DataFrame: DataFrame com os dados transformados.
    """
    #Region ComponentesComite
    composicao_comite = fre_data['AssembleiaGeralEAdm']['ComposicaoComites']['ComponenteComite']
    df_normalized = normalize_json(composicao_comite, 'Participante_ExperienciaCriteriosComposicaoExpProfAGA')
    df_transformed_comite = pd.DataFrame(df_normalized)

    for coluna in df_transformed_comite.columns:
        if 'CargoComite' in coluna:
            nome = coluna.split('_')
            if not len(nome) == 2:
                rename_key(df_transformed_comite, coluna, f'CargoComite_{nome[2]}')
                
            else:
                rename_key(df_transformed_comite, coluna, f'CargoComite_{nome[1]}')

    for coluna_data in df_transformed_comite.columns:
        if 'Data' in coluna_data:
            df_transformed_comite[coluna_data] = convert_format_date(df_transformed_comite[coluna_data])

    int_columns = ['Participante_TotalAdministrador', 'Participante_TotalCondenacao', 'EventualCondenacao_TipoCondenacao', 'CargoComite_TipoComite', 'CargoComite_OrgaoAdministracao', 'CargoComite_CargoOcupado', 'CargoComite_TipoComiteAuditoria']

    for col in df_transformed_comite.columns:
        if col in int_columns:
            df_transformed_comite[col] = df_transformed_comite[col].fillna(0)
            df_transformed_comite[col] = df_transformed_comite[col].apply(lambda x: int(x) if isinstance(x, str) and x.isdigit() else x)

    #endregion
    return df_transformed_comite
