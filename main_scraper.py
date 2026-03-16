"""
Orquestrador principal do scraping.
Executa raspagem completa de todas as plataformas ativas
e depois roda a analise de palavras-chave e nichos.
"""

import os
from datetime import datetime

from dotenv import load_dotenv

from database import init_db, SessionLocal, Plataforma, Reclamacao
from scraper import ReclameAquiScraper, salvar_reclamacoes

load_dotenv()


def executar_scraping_completo():
    print("=" * 60)
    print("INICIANDO SCRAPING COMPLETO")
    print(f"Horario: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
    print("=" * 60)

    init_db()
    session = SessionLocal()
    api_key = os.getenv("FIRECRAWL_API_KEY", "")
    if not api_key:
        print("[ERRO] FIRECRAWL_API_KEY nao configurada.")
        session.close()
        return

    scraper = ReclameAquiScraper(api_key)
    plataformas = session.query(Plataforma).filter_by(ativa=True).all()

    total_novas = total_atualizadas = 0

    for plataforma in plataformas:
        print(f"\nPlataforma: {plataforma.nome}")
        print("-" * 40)

        try:
            reclamacoes = scraper.scrape_plataforma(plataforma)
            novas, atualizadas = salvar_reclamacoes(reclamacoes, session)
            total_novas += novas
            total_atualizadas += atualizadas

            plataforma.ultima_raspagem = datetime.now()
            plataforma.total_reclamacoes = (
                session.query(Reclamacao)
                .filter_by(plataforma_nome=plataforma.nome)
                .count()
            )
            session.commit()
            print(f"  Salvo: {novas} novas | {atualizadas} atualizadas | total {plataforma.total_reclamacoes}")

        except Exception as e:
            print(f"  [ERRO] {plataforma.nome}: {e}")
            import traceback
            traceback.print_exc()
            continue

    session.close()

    print("\n" + "=" * 60)
    print("SCRAPING CONCLUIDO")
    print(f"Total: {total_novas} novas | {total_atualizadas} atualizadas")
    print("=" * 60)

    # Analise automatica apos scraping
    print("\nIniciando analise de palavras-chave e nichos...")
    try:
        from analyzer import processar_analise_completa
        processar_analise_completa()
    except Exception as e:
        print(f"[ERRO] Analise falhou: {e}")

    # Análise com Grok AI (apenas reclamações novas)
    print("\n" + "=" * 60)
    print("ANÁLISE COM GROK AI")
    print("=" * 60)
    try:
        from grok_analyzer import processar_analise_grok
        processar_analise_grok(limite=None, reprocessar=False)
    except Exception as e:
        print(f"[ERRO] Grok falhou: {e}")

    print("\nPROCESSO COMPLETO.")


if __name__ == "__main__":
    executar_scraping_completo()
