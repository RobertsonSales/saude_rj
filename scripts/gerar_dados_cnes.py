#!/usr/bin/env python3
"""
Gerador de dados CNES para Saúde(+)BR
=======================================
Baixa os arquivos ST (Estabelecimentos) do FTP do DATASUS via pysus,
filtra somente unidades vinculadas ao SUS e gera um JSON por estado em:
    data/cnes/{UF}.json

Uso local:
    pip install "pysus>=0.4" pandas pyarrow
    python scripts/gerar_dados_cnes.py

O script é também executado mensalmente pelo GitHub Actions
(.github/workflows/atualizar-cnes.yml) e o resultado é commitado
no repositório, onde o Vercel o serve como arquivo estático.
"""

import json
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("ERRO: pandas não encontrado. Instale: pip install pandas pyarrow")
    sys.exit(1)

# ─── Configuração ─────────────────────────────────────────────────────────────

# Diretório de saída: relativo à raiz do repositório
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "cnes"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

UFS = [
    "AC", "AL", "AM", "AP", "BA", "CE", "DF", "ES", "GO",
    "MA", "MG", "MS", "MT", "PA", "PB", "PE", "PI", "PR",
    "RJ", "RN", "RO", "RR", "RS", "SC", "SE", "SP", "TO",
]

# TP_GESTAO → vinculado ao SUS
# 'M' = Municipal, 'E' = Estadual, 'D' = Dupla, 'S' = Sem gestão (privado)
GESTAO_SUS = {"M", "E", "D"}

# TP_UNIDADE → (nivel, perfil) para o app
TP_UNIDADE_MAP = {
    "01": ("primaria",   "Posto de Saúde"),
    "02": ("primaria",   "Centro de Saúde / UBS"),
    "04": ("secundaria", "Policlínica"),
    "05": ("terciaria",  "Hospital Geral"),
    "06": ("terciaria",  "Hospital Especializado"),
    "07": ("terciaria",  "Hospital Dia"),
    "08": ("secundaria", "Pronto-Socorro Geral"),
    "15": ("secundaria", "Unidade Mista"),
    "20": ("secundaria", "Pronto-Atendimento"),
    "21": ("secundaria", "UPA 24h"),
    "22": ("primaria",   "Consultório Isolado"),
    "32": ("primaria",   "Clínica Especializada"),
    "36": ("secundaria", "Clínica/Centro de Especialidade"),
    "39": ("secundaria", "SADT / Diagnose e Terapia"),
    "43": ("primaria",   "Farmácia"),
    "50": ("primaria",   "Vigilância em Saúde"),
    "61": ("terciaria",  "Centro de Parto Normal"),
    "62": ("terciaria",  "Hospital Dia Isolado"),
    "64": ("secundaria", "Central de Regulação"),
    "65": ("secundaria", "Pronto-Atendimento Especializado"),
    "67": ("primaria",   "Laboratório de Saúde Pública"),
    "69": ("secundaria", "Hemocentro"),
    "70": ("primaria",   "CAPS"),
    "71": ("primaria",   "CASF"),
    "72": ("primaria",   "Saúde Indígena"),
    "73": ("secundaria", "Pronto-Socorro Geral"),
    "74": ("secundaria", "Pronto-Socorro Especializado"),
    "75": ("terciaria",  "Casa de Saúde"),
    "76": ("secundaria", "CEREST"),
    "77": ("primaria",   "Atenção Domiciliar"),
    "78": ("primaria",   "Comunidade Terapêutica"),
    "79": ("primaria",   "Oficina Ortopédica"),
    "80": ("secundaria", "Laboratório de Saúde"),
    "81": ("secundaria", "Centro de Saúde Escola"),
    "82": ("secundaria", "Unidade Móvel de Urgência"),
    "83": ("primaria",   "Academia da Saúde"),
    "84": ("secundaria", "SAMU / Central de Regulação de Urgências"),
    "85": ("primaria",   "SAD - Serviço de Atenção Domiciliar"),
    "86": ("primaria",   "Unidade de Atenção Psicossocial"),
}

# ─── Utilitários ──────────────────────────────────────────────────────────────

def get_competencia():
    """Retorna (ano, mes) com 2 meses de defasagem (lag do DATASUS)."""
    d = datetime.now() - timedelta(days=60)
    return d.year, d.month

def campo(row, candidatos, default=""):
    """Retorna o primeiro campo não-nulo entre os candidatos."""
    for c in candidatos:
        if c in row.index:
            v = row[c]
            if pd.notna(v) and str(v).strip() not in ("", "nan", "None"):
                return str(v).strip()
    return default

def processar_df(df: pd.DataFrame, uf: str) -> list:
    """Filtra e transforma um DataFrame CNES em registros compactos."""

    # Normalizar nomes de colunas para maiúsculas
    df = df.copy()
    df.columns = [c.upper() for c in df.columns]

    # ── Filtro 1: vínculo SUS ─────────────────────────────────────────────
    col_gestao = next(
        (c for c in ["TP_GESTAO", "TP_GESTAO_", "TPGESTAO"] if c in df.columns),
        None,
    )
    if col_gestao:
        df = df[df[col_gestao].isin(GESTAO_SUS)]

    # ── Filtro 2: unidades ativas (sem data de desativação) ───────────────
    for col in ["DT_DESATIVACAO", "DT_DESATIVACAO_", "DTDESATIVACAO"]:
        if col in df.columns:
            df = df[df[col].isna() | df[col].isin(["", "0", "00000000"])]
            break

    registros = []
    for _, row in df.iterrows():
        # Código CNES
        cnes = campo(row, ["CO_UNIDADE", "CNES", "CO_CNES", "COUNIDADE"])
        if not cnes:
            continue
        cnes = cnes.zfill(7)

        # Nome (fantasia > razão social)
        nome = campo(row, ["NO_FANTASIA", "NOFANTASIA"])
        if not nome:
            nome = campo(row, ["NO_RAZAO_SOCIAL", "NO_RAZAO_SOCIAL_", "NORAZAOSOCIAL"])
        if not nome:
            continue
        nome = nome.title()  # capitaliza adequadamente

        # Código IBGE do município (6 dígitos, sem dígito verificador)
        ibge6 = campo(row, ["CO_MUNICIPIO_GESTOR", "CO_MUNICIPIO", "CO_MUN_GESTOR", "COMUNICIPIOGESTOR"])
        ibge6 = ibge6.zfill(6) if ibge6 else ""

        # Tipo e classificação
        tp = campo(row, ["TP_UNIDADE", "TP_UNIDADE_", "TPUNIDADE"], "02")
        tp = tp.zfill(2)
        nivel, perfil = TP_UNIDADE_MAP.get(tp, ("primaria", "Unidade de Saúde"))

        # Gestão (esfera administrativa)
        g = campo(row, ["TP_GESTAO", "TP_GESTAO_", "TPGESTAO"], "M")
        gestao = {"M": "Municipal", "E": "Estadual", "D": "Municipal"}.get(g, "Municipal")

        # Endereço
        logradouro = campo(row, ["NO_LOGRADOURO", "DS_LOGRADOURO", "NOLOGRADOURO"])
        numero = campo(row, ["NU_ENDERECO", "DS_NUMERO", "NUENDERECO"])
        endereco = logradouro
        if numero:
            endereco = f"{logradouro}, {numero}" if logradouro else numero

        bairro = campo(row, ["DS_BAIRRO", "NO_BAIRRO", "DSBAIRRO"])
        cep = campo(row, ["DS_CEP", "NU_CEP", "CO_CEP", "DSCEP"])
        # Remove pontuação do CEP
        cep = "".join(c for c in cep if c.isdigit())

        tel = campo(row, ["NU_TELEFONE", "DS_TELEFONE", "NUTELEFONE"])
        # Remove caracteres não numéricos do telefone
        tel = "".join(c for c in tel if c.isdigit())

        registros.append({
            "cnes": cnes,
            "nome": nome,
            "nivel": nivel,
            "perfil": perfil,
            "gestao": gestao,
            "ibge6": ibge6,
            "logradouro": endereco,
            "bairro": bairro,
            "cep": cep,
            "tel": tel,
        })

    return registros


# ─── Download por UF ─────────────────────────────────────────────────────────

def download_uf(uf: str, year: int, month: int) -> "pd.DataFrame | None":
    """Baixa e retorna o DataFrame CNES ST para o estado e competência dados."""

    # ── Tentativa 1: pysus >= 0.4 (API nova) ─────────────────────────────
    try:
        from pysus.ftp.databases.cnes import CNES  # noqa

        db = CNES().load()

        def _tentar(y, m):
            files = db.get_files(group="ST", uf=uf, year=y, month=m)
            if not files:
                return None
            result = db.download(files[0])
            # pysus >= 0.4 pode retornar path de parquet ou DataFrame
            if isinstance(result, (str, Path)):
                return pd.read_parquet(result)
            return result  # já é DataFrame

        df = _tentar(year, month)
        if df is None:
            # Tenta competência anterior (DATASUS pode ter lag > 2 meses)
            prev = datetime(year, month, 1) - timedelta(days=1)
            df = _tentar(prev.year, prev.month)
        if df is not None:
            return df

    except ImportError:
        pass  # pysus não instalado ou API diferente
    except Exception as e:
        print(f"\n    [pysus>=0.4] falha: {e}", end="")

    # ── Tentativa 2: pysus <= 0.3 (API antiga) ───────────────────────────
    try:
        from pysus.online_data.CNES import download  # noqa

        df = download(uf, year, month, "ST")
        if df is not None and not df.empty:
            return df

        # Tenta competência anterior
        prev = datetime(year, month, 1) - timedelta(days=1)
        df = download(uf, prev.year, prev.month, "ST")
        if df is not None and not df.empty:
            return df

    except ImportError:
        pass
    except Exception as e:
        print(f"\n    [pysus<=0.3] falha: {e}", end="")

    return None


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    year, month = get_competencia()
    print(f"╔══════════════════════════════════════════════════════╗")
    print(f"║  Gerador CNES Saúde(+)BR  —  competência {year}/{month:02d}   ║")
    print(f"╚══════════════════════════════════════════════════════╝\n")

    resumo = {}
    falhas = []

    for uf in UFS:
        print(f"  [{uf}] Baixando...", end=" ", flush=True)
        df = download_uf(uf, year, month)

        if df is None:
            print("✗ FALHOU")
            falhas.append(uf)
            resumo[uf] = 0
            continue

        registros = processar_df(df, uf)
        outfile = OUTPUT_DIR / f"{uf}.json"
        with open(outfile, "w", encoding="utf-8") as f:
            json.dump(registros, f, ensure_ascii=False, separators=(",", ":"))

        kb = outfile.stat().st_size / 1024
        resumo[uf] = len(registros)
        print(f"✓  {len(registros):>4} unidades SUS  ({kb:.0f} KB)")

    # ── Metadados ─────────────────────────────────────────────────────────
    meta = {
        "gerado_em": datetime.now().isoformat(),
        "competencia": f"{year}-{month:02d}",
        "totais": resumo,
        "total_geral": sum(resumo.values()),
        "falhas": falhas,
    }
    with open(OUTPUT_DIR / "_meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    # ── Resumo final ──────────────────────────────────────────────────────
    sucesso = len(UFS) - len(falhas)
    print(f"\n{'─'*54}")
    print(f"  Total: {meta['total_geral']:,} unidades SUS | {sucesso}/{len(UFS)} estados")
    if falhas:
        print(f"  Falhas: {', '.join(falhas)}")
        print(f"  (Re-execute o script ou verifique a instalação do pysus)")
    print(f"  Arquivos em: {OUTPUT_DIR.resolve()}")
    print(f"{'─'*54}")


if __name__ == "__main__":
    main()

