"""
Scraper do Reclame Aqui usando Firecrawl.

Estrutura HTML conhecida da pagina lista-reclamacoes/:
  - Link individual:  a[id="site_bp_lista_ler_reclamacao"]
  - Titulo:           h4[data-testid="compain-title-link"] (atributo title)
  - Descricao:        p filho do mesmo container pai do link
  - Status + Data:    spans no div footer do container
  - ID unico:         extraido da URL /slug_UNIQUEID/
"""

import os
import re
import time
from datetime import datetime, timedelta

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from firecrawl import Firecrawl

from database import Reclamacao, gerar_hash_reclamacao

load_dotenv()

ABAS = {
    "ultimas":         "",
    "nao_respondidas": "?status=NOT_ANSWERED",
    "respondidas":     "?status=ANSWERED",
    "avaliadas":       "?status=EVALUATED",
}

STATUS_LABELS = {
    "Respondida":      "respondida",
    "Não respondida":  "nao_respondida",
    "Nao respondida":  "nao_respondida",  # fallback sem acento
    "Em análise":      "em_analise",
    "Em analise":      "em_analise",
    "Avaliada":        "avaliada",
    "Cancelada":       "cancelada",
}


class ReclameAquiScraper:
    def __init__(self, api_key: str):
        self.app = Firecrawl(api_key=api_key)
        self.delay = 1.2  # segundos entre requests

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _extrair_id_url(self, url: str) -> str:
        """Extrai o ID unico do final da URL: /titulo_ABCDEF/ -> ABCDEF"""
        url = url.rstrip("/")
        parte = url.split("/")[-1]
        if "_" in parte:
            return parte.split("_")[-1]
        return parte

    def _parse_data(self, data_str: str) -> datetime:
        """Converte strings de data relativas/absolutas para datetime."""
        data_str = data_str.strip()
        agora = datetime.now()
        try:
            if "hora" in data_str:
                m = re.search(r"\d+", data_str)
                return agora - timedelta(hours=int(m.group())) if m else agora
            if "dia" in data_str:
                m = re.search(r"\d+", data_str)
                return agora - timedelta(days=int(m.group())) if m else agora
            if "semana" in data_str:
                m = re.search(r"\d+", data_str)
                return agora - timedelta(weeks=int(m.group())) if m else agora
            if "mes" in data_str.lower() or "mes" in data_str:
                m = re.search(r"\d+", data_str)
                return agora - timedelta(days=int(m.group()) * 30) if m else agora
            if "minuto" in data_str:
                m = re.search(r"\d+", data_str)
                return agora - timedelta(minutes=int(m.group())) if m else agora
            if "hoje" in data_str.lower():
                return agora
            if "ontem" in data_str.lower():
                return agora - timedelta(days=1)
            if "/" in data_str:
                return datetime.strptime(data_str[:10], "%d/%m/%Y")
        except Exception:
            pass
        return agora

    def _parse_status(self, texto: str):
        """
        Separa status e data de string concatenada.
        Ex: 'RespondidaHa 2 horas' -> ('respondida', 'Ha 2 horas')
        """
        for label, code in STATUS_LABELS.items():
            if texto.startswith(label):
                resto = texto[len(label):].strip()
                return code, resto
        return "desconhecido", texto

    def construir_url_aba(self, url_base: str, tipo_aba: str) -> str:
        """Constrói URL para cada aba com parâmetro de status."""
        url_base = url_base.rstrip("/") + "/"
        params = {
            "nao_respondidas": "?status=NOT_ANSWERED",
            "respondidas":     "?status=ANSWERED",
            "avaliadas":       "?status=EVALUATED",
        }
        return url_base + params.get(tipo_aba, "")

    # ------------------------------------------------------------------
    # Parser HTML
    # ------------------------------------------------------------------

    def _parse_html(self, html: str, aba_nome: str, plataforma_nome: str) -> list:
        soup = BeautifulSoup(html, "html.parser")
        reclamacoes = []

        links = soup.find_all("a", id="site_bp_lista_ler_reclamacao")
        print(f"    -> {len(links)} reclamacoes no HTML")

        for a in links:
            try:
                url = a.get("href", "")
                if not url:
                    continue
                if url.startswith("/"):
                    url = "https://www.reclameaqui.com.br" + url

                # Titulo
                h4 = a.find("h4", attrs={"data-testid": "compain-title-link"})
                titulo = ""
                if h4:
                    titulo = h4.get("title") or h4.get_text(strip=True)
                if not titulo:
                    titulo = a.get_text(strip=True)

                # Container do item = pai do link <a>
                container = a.parent

                # Descricao: primeiro <p> do container
                p_desc = container.find("p")
                descricao = p_desc.get_text(strip=True) if p_desc else ""
                if "deixe sua" in descricao.lower():
                    descricao = ""

                # Status e Data: spans no footer div
                status_code = aba_nome
                data_raw = ""
                for fd in container.find_all("div", recursive=False):
                    spans = fd.find_all("span", recursive=False)
                    if len(spans) >= 2:
                        status_text = spans[0].get_text(strip=True)
                        data_raw = spans[1].get_text(strip=True)
                        status_code = STATUS_LABELS.get(status_text, aba_nome)
                        break
                    elif len(spans) == 1:
                        texto = spans[0].get_text(strip=True)
                        s, d = self._parse_status(texto)
                        if s != "desconhecido":
                            status_code, data_raw = s, d
                            break

                data_reclamacao = self._parse_data(data_raw)
                ra_id = self._extrair_id_url(url)
                hash_id = gerar_hash_reclamacao(ra_id, plataforma_nome)

                reclamacoes.append({
                    "hash_id":         hash_id,
                    "plataforma_nome": plataforma_nome,
                    "titulo":          titulo,
                    "descricao":       descricao,
                    "data_reclamacao": data_reclamacao,
                    "status":          status_code,
                    "aba_origem":      aba_nome,
                    "url_original":    url,
                })

            except Exception as e:
                print(f"    [WARN] Erro ao parsear item: {e}")
                continue

        return reclamacoes

    # ------------------------------------------------------------------
    # Scraping com paginação completa
    # ------------------------------------------------------------------

    def scrape_aba_completa(self, url_base: str, aba_nome: str, plataforma_nome: str) -> list:
        """
        Usa Firecrawl /crawl para seguir TODAS as páginas automaticamente.
        Fallback: paginação manual.
        """
        print(f"  Iniciando crawl: {plataforma_nome} - {aba_nome}")
        print(f"  URL: {url_base}")

        try:
            resultado_crawl = self.app.crawl_url(
                url_base,
                limit=500,
                scrape_options={
                    "formats": ["html"],
                    "waitFor": 5000,
                },
                max_depth=2,
                allow_backward_links=True,
                allow_external_links=False,
                ignore_sitemap=True,
            )

            # SDK v4+ retorna objeto com atributo .data (lista de ScrapeResponse)
            paginas = getattr(resultado_crawl, "data", None)
            if paginas is None and isinstance(resultado_crawl, dict):
                paginas = resultado_crawl.get("data", [])

            if not paginas:
                raise ValueError("crawl_url retornou 0 páginas")

            print(f"  Crawl concluído: {len(paginas)} páginas encontradas")

            todas_reclamacoes = []
            for pagina in paginas:
                html = getattr(pagina, "html", None) or (pagina.get("html", "") if isinstance(pagina, dict) else "")
                if not html:
                    continue
                recs = self._parse_html(html, aba_nome, plataforma_nome)
                if recs:
                    todas_reclamacoes.extend(recs)

            print(f"  Total extraído (crawl): {len(todas_reclamacoes)} reclamações")
            return todas_reclamacoes

        except Exception as e:
            print(f"  [WARN] crawl_url falhou: {e}")
            print("  Tentando paginação manual...")
            return self.scrape_aba_paginacao_manual(url_base, aba_nome, plataforma_nome)

    def scrape_aba_paginacao_manual(self, url_base: str, aba_nome: str, plataforma_nome: str) -> list:
        """
        Fallback: itera páginas manualmente adicionando ?pagina=N.
        Para quando encontra 3 páginas vazias consecutivas.
        """
        todas_reclamacoes = []
        pagina = 1
        vazias_consecutivas = 0
        max_paginas = 200

        # Separa URL base do parâmetro de status (se houver)
        if "?" in url_base:
            url_sem_qs, qs = url_base.split("?", 1)
            url_sem_qs = url_sem_qs.rstrip("/") + "/"
        else:
            url_sem_qs = url_base.rstrip("/") + "/"
            qs = ""

        while pagina <= max_paginas:
            if qs:
                url = f"{url_sem_qs}?{qs}&pagina={pagina}"
            else:
                url = f"{url_sem_qs}?pagina={pagina}"

            print(f"  [{aba_nome}] Página {pagina}: {url}")

            try:
                result = self.app.scrape(
                    url,
                    formats=["html"],
                    wait_for=5000,
                    timeout=60000,
                )
                html = result.html or ""
                recs = self._parse_html(html, aba_nome, plataforma_nome)

                if recs:
                    print(f"    -> {len(recs)} reclamações na página {pagina}")
                    todas_reclamacoes.extend(recs)
                    vazias_consecutivas = 0
                else:
                    vazias_consecutivas += 1
                    print(f"    -> Página vazia ({vazias_consecutivas}/3)")
                    if vazias_consecutivas >= 3:
                        print(f"  Fim detectado após {pagina} páginas")
                        break

            except Exception as e:
                print(f"  [ERRO] Página {pagina}: {e}")
                vazias_consecutivas += 1
                if vazias_consecutivas >= 3:
                    break

            pagina += 1
            time.sleep(2)

        print(f"  Total extraído (paginação manual): {len(todas_reclamacoes)} reclamações")
        return todas_reclamacoes

    def scrape_plataforma(self, plataforma) -> list:
        """Raspa TODAS as abas de uma plataforma com paginação completa."""
        todas = []
        url_base = plataforma.url_base.rstrip("/") + "/"

        abas_config = {
            "ultimas":         url_base,
            "nao_respondidas": self.construir_url_aba(url_base, "nao_respondidas"),
            "respondidas":     self.construir_url_aba(url_base, "respondidas"),
            "avaliadas":       self.construir_url_aba(url_base, "avaliadas"),
        }

        for aba_nome, url in abas_config.items():
            print(f"\n{'='*60}")
            print(f"  ABA: {aba_nome.upper()} — {plataforma.nome}")
            print(f"{'='*60}")

            try:
                reclamacoes = self.scrape_aba_completa(url, aba_nome, plataforma.nome)
                todas.extend(reclamacoes)
            except Exception as e:
                print(f"  [ERRO] Aba {aba_nome}: {e}")

            time.sleep(3)

        return todas


# ------------------------------------------------------------------
# Persistencia
# ------------------------------------------------------------------

def salvar_reclamacoes(reclamacoes: list, session) -> tuple:
    novas = atualizadas = 0
    vistos_batch = set()  # evita duplicatas dentro do proprio batch (mesma rec em abas diferentes)

    for r in reclamacoes:
        hid = r["hash_id"]
        if hid in vistos_batch:
            continue
        vistos_batch.add(hid)

        existe = session.query(Reclamacao).filter_by(hash_id=hid).first()
        if existe:
            existe.ultima_atualizacao = datetime.now()
            atualizadas += 1
        else:
            session.add(Reclamacao(**r))
            novas += 1

    session.commit()
    return novas, atualizadas
