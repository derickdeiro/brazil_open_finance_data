import pandera.pandas as pa
from projects.core.schema_validator import is_valid_date_format


fre_descricao_caracteristicas_schema = pa.DataFrameSchema(
    {
        'OrgaoAdministracao': pa.Column(
            pa.String,
            nullable=False,
            coerce=True
        ),
        'Amarelo': pa.Column(
            pa.Int,
            nullable=False,
            coerce=True
        ),
        'Branco': pa.Column(
            pa.Int,
            nullable=False,
            coerce=True
        ),
        'Preto': pa.Column(
            pa.Int,
            nullable=False,
            coerce=True
        ),
        'Pardo': pa.Column(
            pa.Int,
            nullable=False,
            coerce=True
        ),
        'Indigena': pa.Column(
            pa.Int,
            nullable=False,
            coerce=True
        ),
        'Outros_Cor': pa.Column(
            pa.Int,
            nullable=False,
            coerce=True
        ),
        'PrefereNaoResponder_Cor': pa.Column(
            pa.Int,
            nullable=False,
            coerce=True
        ),
        'NaoSeAplica_Cor': pa.Column(
            pa.Bool,
            nullable=False,
            coerce=True
        ),
        'Masculino': pa.Column(
            pa.Int,
            nullable=False,
            coerce=True
        ),
        'Feminino': pa.Column(
            pa.Int,
            nullable=False,
            coerce=True
        ),
        'NaoBinario': pa.Column(
            pa.Int,
            nullable=False,
            coerce=True
        ),
        'Outros_Genero': pa.Column(
            pa.Int,
            nullable=False,
            coerce=True
        ),
        'PrefereNaoResponder_Genero': pa.Column(
            pa.Int,
            nullable=False,
            coerce=True
        ),
        'NaoSeAplica_Genero': pa.Column(
            pa.Bool,
            nullable=False,
            coerce=True
        ),
    }
)

fre_experiencia_profissional_schema = pa.DataFrameSchema(
    {
        'Administrador_Nome': pa.Column(
            pa.String,
            nullable=False,
            coerce=True
        ),
        'Administrador_CpfPassaporte': pa.Column(
            pa.String,
            nullable=True,
            coerce=True
        ),
        'Administrador_DataNascimento': pa.Column(
            pa.String, 
            checks=pa.Check(
                lambda s: is_valid_date_format(s),
                error="DATASER diferente do esperado: '%Y-%m-%d'",
            ),
            nullable=False  # Ensures no null values are present
        ),
        'Administrador_Profissao': pa.Column(
            pa.String,
            nullable=False,
            coerce=True
        ),
        'Administrador_ExperienciaProfissional': pa.Column(
            pa.String,
            nullable=False,
            coerce=True
        ),
        'MembroDiretoriaCAdmCF_OrgaoAdm': pa.Column(
            pa.Int,
            nullable=False,
            coerce=True
        ),
        'MembroDiretoriaCAdmCF_CargoEletivoOcupado': pa.Column(
            pa.String,
            nullable=True,
            coerce=True
        ),
        'MembroDiretoriaCAdmCF_DescricaoOutroCargo': pa.Column(
            pa.String,
            nullable=True,
            coerce=True
        ),
        'MembroDiretoriaCAdmCF_DataEleicao': pa.Column(
            pa.String, 
            checks=pa.Check(
                lambda s: is_valid_date_format(s),
                error="DATASER diferente do esperado: '%Y-%m-%d'",
            ),
            nullable=False  # Ensures no null values are present
        ),
        'MembroDiretoriaCAdmCF_DataPosse': pa.Column(
            pa.String, 
            checks=pa.Check(
                lambda s: is_valid_date_format(s),
                error="DATASER diferente do esperado: '%Y-%m-%d'",
            ),
            nullable=False  # Ensures no null values are present
        ),
        'MembroDiretoriaCAdmCF_PrazoMandato': pa.Column(
            pa.String,
            nullable=False,
            coerce=True
        ),
        'MembroDiretoriaCAdmCF_EleitoPeloControlador': pa.Column(
            pa.Int,
            nullable=False,
            coerce=True
        ),
        'Condenacao_TipoCondenacao': pa.Column(
            pa.String,
            nullable=True,
            coerce=True
        ),
    }
)

fre_composicao_comite_schema = pa.DataFrameSchema(
    {
        'Participante_NomeCampo': pa.Column(
            pa.String,
            nullable=False,
            coerce=True
        ),
        'Participante_Cpf': pa.Column(
            pa.String,
            nullable=True,
            coerce=True
        ),
        'Participante_DataNascimento': pa.Column(
            pa.String, 
            checks=pa.Check(
                lambda s: is_valid_date_format(s),
                error="DATASER diferente do esperado: '%Y-%m-%d'",
            ),
            nullable=False  # Ensures no null values are present
        ),
        'Participante_Profissao': pa.Column(
            pa.String,
            nullable=False,
            coerce=True
        ),
        'Participante_ExperienciaCriteriosComposicaoExpProfAGA': pa.Column(
            pa.String,
            nullable=False,
            coerce=True
        ),
        'Participante_TotalAdministrador': pa.Column(
            pa.Int,
            nullable=False,
            coerce=True
        ),
        'Participante_TotalCondenacao': pa.Column(
            pa.Int,
            nullable=False,
            coerce=True
        ),
        'Participante_Passaporte': pa.Column(
            pa.String,
            nullable=True,
            coerce=True
        ),
        'Participante_Nacionalidade': pa.Column(
            pa.String,
            nullable=False,
            coerce=True
        ),
        'EventualCondenacao_TipoCondenacao': pa.Column(
            pa.Int,
            nullable=True,
            # coerce=True
            
        ),
        'CargoComite_CargoOcupado': pa.Column(
            pa.Float,
            nullable=True,
            coerce=True
        ),
        'CargoComite_DescricaoOutrosCargos': pa.Column(
            pa.String,
            nullable=True,
            coerce=True
        ),
        'CargoComite_DataEleicao': pa.Column(
            pa.String, 
            checks=pa.Check(
                lambda s: is_valid_date_format(s),
                error="DATASER diferente do esperado: '%Y-%m-%d'",
            ),
            nullable=True
        ),
        'CargoComite_DataPosse': pa.Column(
            pa.String, 
            checks=pa.Check(
                lambda s: is_valid_date_format(s),
                error="DATASER diferente do esperado: '%Y-%m-%d'",
            ),
            nullable=True
        ),
        'CargoComite_PrazoMandato': pa.Column(
            pa.String,
            nullable=True,
            coerce=True
        ),
        'CargoComite_DataInicioPrimeiroMandato': pa.Column(
            pa.String, 
            checks=pa.Check(
                lambda s: is_valid_date_format(s),
                error="DATASER diferente do esperado: '%Y-%m-%d'",
            ),
            nullable=True
        ),
        'CargoComite_FoiEleitoPeloControladorComite': pa.Column(
            pa.String,
            nullable=True,
            coerce=True
        ),
        'CargoComite_TipoComite': pa.Column(
            pa.String,
            nullable=True,
            coerce=True
        ),
        'CargoComite_TipoComiteAuditoria': pa.Column(
            pa.String,
            nullable=True,
            coerce=True,
            required=False
        ),
         'CargoComite_DescricaoOutrosComites': pa.Column(
            pa.String,
            nullable=True,
            coerce=True,
            required=False
        ),
    }
)