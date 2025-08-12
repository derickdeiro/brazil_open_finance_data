import pandas as pd
from projects.data_acquisition.fre.fre_utils import (
    remove_comma_to_float,
    remove_thousands_separator,
    convert_format_date
    )
from typing import Dict

def transform_posicao_acionaria(fre_data: Dict) -> pd.DataFrame:
    """
    Transforms the 'PosicaoAcionaria' data from the FRE data into a DataFrame.
    
    Args:
        fre_data (Dict): The FRE data containing 'PosicaoAcionaria'.
        
    Returns:
        pd.DataFrame: A DataFrame containing the transformed 'PosicaoAcionaria' data.
    """
            
    posicao_acionaria_content = fre_data['ControleGrupoEconomico']['PosicaoAcionaria']
    
    df_normalized = pd.json_normalize(posicao_acionaria_content, sep='_')
    
    df_transformed = df_normalized.copy(deep=True)
    
    df_transformed.columns = [col if '_' not in col else col.split('_')[-1] for col in df_transformed.columns]
            
    float_columns = ['AcoesOrdinariasPorcent', 'AcoesPreferenciaisPorcent', 'TotalAcoesPorcent']
    int_columns = ['NumeroIdentificadorAcionista', 'NumeroIdentificadorAcionistaPai', 'QtdAcoesOrdinaria', 'QtdAcoesPreferenciais', 'QtdTotalAcoes']
    
    for col in float_columns:
        df_transformed[col] = df_transformed[col].apply(remove_comma_to_float)
    
    for col in int_columns:
        df_transformed[col] = df_transformed[col].apply(remove_thousands_separator)
    
    df_transformed['UF'] = df_transformed['UF'].apply(lambda x: x[0] if isinstance(x, list) else x)
    df_transformed['UltimaAlteracao'] = df_transformed['UltimaAlteracao'].apply(convert_format_date)
    
    return df_transformed