"""
Processador batch de reclamações usando Grok AI.
"""

import json
from collections import Counter
from datetime import datetime

from database import SessionLocal, Reclamacao, Produto
from grok_client import GrokAI


def processar_analise_grok(limite: int = None, reprocessar: bool = False):
    """
    Processa reclamações com Grok AI: detecta produtos nichados, nichos e score.

    Args:
        limite: máximo de reclamações a processar (None = todas)
        reprocessar: se True, reanalisa reclamações já processadas
    """
    session = SessionLocal()
    grok = GrokAI()

    if not grok.api_key:
        print("[Grok] GROK_API_KEY não configurada. Abortando.")
        session.close()
        return

    print("=" * 60)
    print("ANÁLISE COM GROK AI")
    print("=" * 60)

    query = session.query(Reclamacao)

    if not reprocessar:
        query = query.filter(Reclamacao.produto_mencionado == None)

    if limite:
        query = query.limit(limite)

    reclamacoes = query.all()
    total = len(reclamacoes)

    if not total:
        print("Nenhuma reclamação para analisar.")
        session.close()
        return

    print(f"Total para analisar: {total}")

    processadas = 0
    produtos_encontrados = []

    for i, rec in enumerate(reclamacoes):
        try:
            print(f"\n[{i+1}/{total}] {rec.titulo[:60]}...")

            analise = grok.analisar_reclamacao(rec.titulo, rec.descricao or "")

            if not analise:
                print("  -> Análise falhou, pulando")
                continue

            rec.produto_mencionado = analise.get("produto_mencionado")
            rec.nicho_detectado = analise.get("nicho") or rec.nicho_detectado
            rec.palavras_chave_extraidas = json.dumps(analise, ensure_ascii=False)

            if analise.get("e_produto_nichado") and rec.produto_mencionado:
                score = analise.get("score_oportunidade", 0)
                print(f"  NICHADO: {rec.produto_mencionado} | {analise.get('nicho')} | score {score}/10")
                produtos_encontrados.append({
                    "nome": rec.produto_mencionado,
                    "nicho": analise.get("nicho", "desconhecido"),
                    "score": score,
                    "plataforma": rec.plataforma_nome,
                })
            else:
                print(f"  Genérico: {rec.produto_mencionado or 'N/A'}")

            processadas += 1

            if processadas % 10 == 0:
                session.commit()
                print(f"  [checkpoint] {processadas} salvas")

        except Exception as e:
            print(f"  [ERRO] {e}")
            continue

    session.commit()

    # Atualiza tabela de produtos
    _atualizar_produtos(session, produtos_encontrados)

    session.close()

    print("\n" + "=" * 60)
    print(f"CONCLUÍDO: {processadas}/{total} analisadas | {len(produtos_encontrados)} produtos nichados")
    print("=" * 60)


def _atualizar_produtos(session, produtos_lista: list):
    """Agrupa produtos por nome e atualiza a tabela produtos."""
    if not produtos_lista:
        return

    # Agrupa por nome
    agrupado: dict[str, dict] = {}
    for p in produtos_lista:
        nome = p["nome"]
        if nome not in agrupado:
            agrupado[nome] = {"nicho": p["nicho"], "frequencia": 0, "plataformas": set()}
        agrupado[nome]["frequencia"] += 1
        agrupado[nome]["plataformas"].add(p["plataforma"])

    for nome, dados in agrupado.items():
        produto = session.query(Produto).filter_by(nome=nome).first()
        plataformas_json = json.dumps(list(dados["plataformas"]), ensure_ascii=False)

        if produto:
            produto.frequencia += dados["frequencia"]
            produto.ultima_deteccao = datetime.now()
        else:
            session.add(Produto(
                nome=nome,
                frequencia=dados["frequencia"],
                nicho_provavel=dados["nicho"],
                plataformas_vendido=plataformas_json,
                primeira_deteccao=datetime.now(),
                ultima_deteccao=datetime.now(),
            ))

    session.commit()
    print(f"  Produtos atualizados: {len(agrupado)}")
