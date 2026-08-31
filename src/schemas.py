from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class BankValidator(BaseModel):
    """Trusted schema for `EnquadramentoInicia_v2.tsv` (Bancos)."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    segmento: Literal["S1", "S2", "S3", "S4", "S5"] = Field(alias="Segmento")
    cnpj_base: str = Field(alias="CNPJ", pattern=r"^\d{8}$")
    nome_instituicao: str = Field(alias="Nome", max_length=200)

    # Exclusive from model
    tipo_registro: Literal["PRUDENCIAL", "INSTITUICAO"] | None = None
    nome_tem_caractere_invalido: bool = False

    @model_validator(mode="before")
    @classmethod
    def normalize_raw(cls, data: dict) -> dict:
        cnpj = (data.get("CNPJ") or "").strip()
        if not cnpj:
            raise ValueError("CNPJ vazio")
        data["CNPJ"] = cnpj.zfill(8)
        data["Nome"] = (data.get("Nome") or "").strip()
        return data

    @model_validator(mode="after")
    def deriva_campos(self) -> "BankValidator":
        # Mojibake da origem: U+FFFD (cp1252 lido como UTF-8) e '?' literal.
        self.nome_tem_caractere_invalido = (
            "�" in self.nome_instituicao or "?" in self.nome_instituicao
        )
        # CNPJ raiz repetido entre conglomerado e instituição individual;
        # o sufixo "- PRUDENCIAL" é o único sinal que os distingue na origem.
        self.tipo_registro = (
            "PRUDENCIAL"
            if self.nome_instituicao.upper().endswith("- PRUDENCIAL")
            else "INSTITUICAO"
        )
        return self


class ComplaintsValidator(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    ano: int                                             = Field(alias="Ano")
    trimestre: int                                       = Field(alias="Trimestre")
    categoria: str                                       = Field(alias="Categoria")
    tipo: Literal["Conglomerado", "Banco/financeira"]    = Field(alias="Tipo")
    cnpj_base: str | None                                = Field(alias="CNPJ IF", pattern=r"^\d{8}$")
    instituicao_financeira: str                          = Field(alias="Instituição financeira")
    indice: float | None                                 = Field(alias="Índice")
    qtd_recl_reguladas_procedentes: int                  = Field(alias="Quantidade de reclamações reguladas procedentes")
    qtd_recl_reguladas_outras: int                       = Field(alias="Quantidade de reclamações reguladas - outras")
    qtd_recl_nao_reguladas: int                          = Field(alias="Quantidade de reclamações não reguladas")
    qtd_recl_total: int                                  = Field(alias="Quantidade total de reclamações")
    qtd_clientes_ccs_scr: int | None                     = Field(alias="Quantidade total de clientes – CCS e SCR")
    qtd_clientes_ccs: int | None                         = Field(alias="Quantidade de clientes – CCS")
    qtd_clientes_scr: int | None                         = Field(alias="Quantidade de clientes – SCR")

    @model_validator(mode="before")
    @classmethod
    def normalize_raw(cls, data: dict) -> dict:

        cnpj = (data.get("CNPJ IF") or "").strip()
        if cnpj:
            data["CNPJ IF"] = cnpj.zfill(8)
        else:
            data["CNPJ IF"] = None

        data["Categoria"] = (data.get("Categoria") or "").strip()
        data["Instituição financeira"] = (data.get("Instituição financeira") or "").strip()
        indice = (data.get("Índice") or "").strip()
        data["Índice"] = float(indice.replace(".", "").replace(",", ".")) if indice else None

        qtd_cli_ccs_scr = (data.get("Quantidade total de clientes – CCS e SCR").strip())
        qtd_cli_ccs = (data.get("Quantidade de clientes – CCS").strip())
        qtd_cli_scr = (data.get("Quantidade de clientes – SCR").strip())

        if not qtd_cli_ccs_scr:
            data["Quantidade total de clientes – CCS e SCR"] = None
        if not qtd_cli_ccs:
            data["Quantidade de clientes – CCS"] = None
        if not qtd_cli_scr:
            data["Quantidade de clientes – SCR"] = None

        data["Trimestre"] = int((data.get("Trimestre") or "").strip().replace("º", ""))

        return data

    @model_validator(mode="after")
    def validate_business_rules(self) -> "ComplaintsValidator":
        sum = (
            self.qtd_recl_reguladas_procedentes
            + self.qtd_recl_reguladas_outras
            + self.qtd_recl_nao_reguladas
        )
        if sum != self.qtd_recl_total:
            raise ValueError(
                f"Soma das reclamações ({sum}) não bate com o total ({self.qtd_recl_total})"
            )

        tem_cnpj = self.cnpj_base is not None
        if self.tipo == "Banco/financeira" and not tem_cnpj:
            raise ValueError("CNPJ obrigatório quando Tipo == 'Banco/financeira'")
        if self.tipo == "Conglomerado" and tem_cnpj:
            raise ValueError("Conglomerado não deveria ter CNPJ IF preenchido")

        return self


class EmployerValidator(BaseModel):
    """Trusted schema for the 22 columns shared by both Empregados files.

    Not meant to be instantiated directly — real rows always carry either
    `Segmento` or `CNPJ` as the 23rd column, so use `EmployerSegmentoValidator`
    (v2 file) or `EmployerCnpjValidator` (less_v2 file).
    """

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    employer_name: str                  = Field(alias="employer_name", min_length=1)
    reviews_count: int                  = Field(alias="reviews_count", ge=0)
    culture_count: int                  = Field(alias="culture_count", ge=0)
    salaries_count: int                 = Field(alias="salaries_count", ge=0)
    benefits_count: int                 = Field(alias="benefits_count", ge=0)
    employer_website: str               = Field(alias="employer-website")
    employer_headquarters: str          = Field(alias="employer-headquarters")
    employer_founded: int | None        = Field(alias="employer-founded")
    employer_industry: str              = Field(alias="employer-industry")
    employer_revenue: str               = Field(alias="employer-revenue")
    url: str                            = Field(alias="url")
    nota_geral: float                   = Field(alias="Geral")
    nota_cultura_valores: float         = Field(alias="Cultura e valores")
    nota_diversidade_inclusao: float    = Field(alias="Diversidade e inclusão")
    nota_qualidade_vida: float          = Field(alias="Qualidade de vida")
    nota_alta_lideranca: float          = Field(alias="Alta liderança")
    nota_remuneracao_beneficios: float  = Field(alias="Remuneração e benefícios")
    nota_oportunidades_carreira: float  = Field(alias="Oportunidades de carreira")
    pct_recomendam: float               = Field(alias="Recomendam para outras pessoas(%)", ge=0, le=100)
    pct_perspectiva_positiva: float     = Field(alias="Perspectiva positiva da empresa(%)", ge=0, le=100)
    nome_instituicao: str               = Field(alias="Nome", min_length=1)
    match_percent: int                  = Field(alias="match_percent", ge=0, le=100)

    # Exclusive from model
    employer_revenue_desconhecida: bool = False

    @model_validator(mode="before")
    @classmethod
    def normalize_raw(cls, data: dict) -> dict:
        data["employer_name"] = (data.get("employer_name") or "").strip()
        data["Nome"] = (data.get("Nome") or "").strip()

        founded = (data.get("employer-founded") or "").strip()
        data["employer-founded"] = int(float(founded)) if founded else None

        revenue = (data.get("employer-revenue") or "").strip()
        if revenue.lower() == "desconhecida/não se aplica":
            revenue = "Desconhecido/Não se aplica"
        data["employer-revenue"] = revenue

        return data

    @model_validator(mode="after")
    def derive_fields(self) -> "EmployerValidator":
        self.employer_revenue_desconhecida = (
            self.employer_revenue == "Desconhecido/Não se aplica"
        )
        return self


class EmployerSegmentoValidator(EmployerValidator):
    """`glassdoor_consolidado_join_match_v2.csv` — carrega `Segmento`."""

    segmento: Literal["S1", "S2", "S3"] = Field(alias="Segmento")


class EmployerCnpjValidator(EmployerValidator):
    """`glassdoor_consolidado_join_match_less_v2.csv` — carrega `CNPJ`."""

    cnpj_base: str = Field(alias="CNPJ", pattern=r"^\d{8}$")

    @model_validator(mode="before")
    @classmethod
    def normalize_cnpj(cls, data: dict) -> dict:
        cnpj = (data.get("CNPJ") or "").strip()
        if not cnpj:
            raise ValueError("CNPJ vazio")
        data["CNPJ"] = cnpj.zfill(8)
        return data
