import pandas as pd
from projects.data_acquisition.fre.fre_utils import (
    remove_comma_to_float,
    convert_format_date,
)
from typing import Dict


def transform_remuneracao_min_med_max(fre_data: Dict) -> pd.DataFrame:
    """
    Transform the 'remuneracao' data into the desired format.
    Args:
        fre_data (dict): The JSON data to be transformed.
    Returns:
        pd.DataFrame: 'RemunMinMedMax'.
    """

    #Region RemumMinMedMax

    if 'exerciciosSociaisField' not in fre_data['RemuneracaoAdministradores']['RemunMinMedMax'].keys():
        remun_min_med_max = pd.DataFrame()
        
    if 'exerciciosSociaisField' in fre_data['RemuneracaoAdministradores']['RemunMinMedMax'].keys():
        json_data_remuMaxMid = fre_data['RemuneracaoAdministradores']['RemunMinMedMax']['exerciciosSociaisField']        
        remun_min_med_max = pd.DataFrame(json_data_remuMaxMid)

        remun_min_med_max['ExercicioSocial'] = remun_min_med_max["ExercicioSocial"].apply(convert_format_date)

        thousands_separator = ['NumTotalMembros', 'NumMembrosRemun', 'MaiorRemun', 'MenorRemun', 'MediaRemun']
        for col in thousands_separator:           
            remun_min_med_max[col] = remun_min_med_max[col].apply(remove_comma_to_float)

    #endregion
    return remun_min_med_max


def transform_remuneracao_total_orgao(fre_data: Dict) -> pd.DataFrame:
    """
    Transform the 'remuneracao' data into the desired format.
    Args:
        fre_data (dict): The JSON data to be transformed.
    Returns:
        pd.DataFrame: 'RemunTotalOrgao'.
    """
    
    #Region RemunOrgao

    if 'ExerciciosSociais' not in fre_data['RemuneracaoAdministradores']['RemuneracaoTotalOrgao'].keys():
        remun_orgao = pd.DataFrame()
        
    if 'ExerciciosSociais' in fre_data['RemuneracaoAdministradores']['RemuneracaoTotalOrgao'].keys():
        json_data_remunorgao = fre_data['RemuneracaoAdministradores']['RemuneracaoTotalOrgao']['ExerciciosSociais']
        remun_orgao = pd.DataFrame()

        for i in range(len(json_data_remunorgao)):
            data = json_data_remunorgao[i]['ExercicioSocial']

            for j in json_data_remunorgao[i]['RemunOrgao']:
                df_normalize = pd.json_normalize(j, sep='_')
                df_normalize['ExercicioSocial'] = data
                remun_orgao = pd.concat([remun_orgao, df_normalize])            

        remun_orgao.columns = [col if '_' not in col else col.split('_')[1] for col in remun_orgao.columns]
        remun_orgao = remun_orgao.reset_index(drop=True)

        thousands_separator = ['NumTotalMembros', 'NumMembrosRemun', 'ValorTotalRemunOrgao', 'Salário', 'Beneficios', 
                                'ParticipacaoComites', 'ParticipacaoResultados', 'Bonus', 'ParticipacaoReunioes', 'Comissoes',
                                'PosEmprego', 'CessacaoCargo', 'BaseadoAcoes']
        for col in thousands_separator:
            remun_orgao[col] = remun_orgao[col].apply(remove_comma_to_float)

        remun_orgao['ExercicioSocial'] = remun_orgao["ExercicioSocial"].apply(convert_format_date)
        #endregion
    return remun_orgao


def transform_remuneracao_variavel(fre_data: Dict) -> pd.DataFrame:
    """
    Transform the 'remuneracao' data into the desired format.
    Args:
        fre_data (dict): The JSON data to be transformed.
    Returns:
        pd.DataFrame: 'RemunVariavelOrgao'.
    """
    #Region RemunVariavel
    if 'ExerciciosSociais' not in fre_data['RemuneracaoAdministradores']['RemuneracaoVariavelOrgao'].keys():
        remun_variavel = pd.DataFrame()
        
    if 'ExerciciosSociais' in fre_data['RemuneracaoAdministradores']['RemuneracaoVariavelOrgao'].keys():

        json_data_variavel = fre_data['RemuneracaoAdministradores']['RemuneracaoVariavelOrgao']['ExerciciosSociais']
        remun_variavel = pd.DataFrame()

        for i in range(len(json_data_variavel)):
            data = json_data_variavel[i]['ExercicioSocial']
            
            for j in json_data_variavel[i]['RemunVariavelOrgao']:
                df_normalize = pd.json_normalize(j, sep='_')
                df_normalize['ExercicioSocial'] = data
                remun_variavel = pd.concat([remun_variavel, df_normalize])

        remun_variavel.columns = [col if '_' not in col else col.replace('_', '') for col in remun_variavel.columns]
        
        remun_variavel = remun_variavel.reset_index(drop=True)  

        thousands_separator = ['NumTotalMembros', 'NumMembrosRemun',
                                'BonusValorMinimoPrevistoPlanoRemuneracao', 
                                'BonusValorMaximoPrevistoPlanoRemuneracao', 
                                'BonusValorPrevistoPlanoRemuneracaoMetaAtingida',
                                'BonusValorReconhecidoUltimo3ExerciciosSociais', 
                                'ParticipacaoResultadoValorMinimoPrevistoPlanoRemuneracao',
                                'ParticipacaoResultadoValorMaximoPrevistoPlanoRemuneracao',
                                'ParticipacaoResultadoValorPrevistoPlanoRemuneracaoMetaAtingida',
                                'ParticipacaoResultadoValorReconhecidoUltimo3ExerciciosSociais']
        for col in thousands_separator:
            remun_variavel[col] = remun_variavel[col].apply(remove_comma_to_float)
        
        remun_variavel['ExercicioSocial'] = remun_variavel["ExercicioSocial"].apply(convert_format_date)

        #endregion

    return remun_variavel