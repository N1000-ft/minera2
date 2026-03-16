"""
API FastAPI + interface web do agregador de reclamacoes.
"""

import csv
import io
import json
import os
import re
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from dotenv import load_dotenv
from fastapi import FastAPI, Query, BackgroundTasks, HTTPException
from fastapi.responses import HTMLResponse, Response, StreamingResponse
from pydantic import BaseModel
from sqlalchemy import desc, func, cast, Date

from database import init_db, SessionLocal, Reclamacao, Plataforma, PalavraChave, Nicho, Stopword, Produto
from scheduler import iniciar_scheduler

load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    iniciar_scheduler()
    yield


app = FastAPI(title="Agregador de Reclamacoes", lifespan=lifespan)


def _html(nome: str) -> str:
    path = os.path.join(os.path.dirname(__file__), "templates", nome)
    with open(path, encoding="utf-8") as f:
        return f.read()


# ------------------------------------------------------------------
# Paginas HTML
# ------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def home():
    return _html("index.html")


@app.get("/plataformas", response_class=HTMLResponse)
async def pagina_plataformas():
    return _html("plataformas.html")


# ------------------------------------------------------------------
# Reclamacoes
# ------------------------------------------------------------------

@app.get("/api/reclamacoes")
async def listar_reclamacoes(
    plataforma: str = Query(None),
    status: str = Query(None),
    aba: str = Query(None),
    nicho: str = Query(None),
    busca: str = Query(None),
    limit: int = Query(50, le=200),
    offset: int = Query(0),
):
    session = SessionLocal()
    query = session.query(Reclamacao)

    if plataforma:
        query = query.filter(Reclamacao.plataforma_nome == plataforma)
    if status:
        query = query.filter(Reclamacao.status == status)
    if aba:
        query = query.filter(Reclamacao.aba_origem == aba)
    if nicho:
        query = query.filter(Reclamacao.nicho_detectado == nicho)
    if busca:
        pat = f"%{busca}%"
        query = query.filter(
            (Reclamacao.titulo.ilike(pat)) | (Reclamacao.descricao.ilike(pat))
        )

    total = query.count()
    rows = (
        query.order_by(desc(Reclamacao.data_reclamacao))
        .limit(limit)
        .offset(offset)
        .all()
    )
    session.close()

    return {
        "total": total,
        "limit": limit,
        "offset": offset,
        "reclamacoes": [
            {
                "id":         r.id,
                "titulo":     r.titulo,
                "descricao":  (r.descricao or "")[:300],
                "plataforma": r.plataforma_nome,
                "nicho":      r.nicho_detectado,
                "data":       r.data_reclamacao.isoformat() if r.data_reclamacao else None,
                "status":     r.status,
                "aba":        r.aba_origem,
                "url":        r.url_original,
                "coletado":   r.primeira_coleta.isoformat() if r.primeira_coleta else None,
            }
            for r in rows
        ],
    }


# ------------------------------------------------------------------
# Estatisticas
# ------------------------------------------------------------------

@app.get("/api/estatisticas")
async def estatisticas():
    session = SessionLocal()

    total = session.query(Reclamacao).count()

    recentes_24h = session.query(Reclamacao).filter(
        Reclamacao.primeira_coleta >= datetime.now() - timedelta(days=1)
    ).count()

    por_plataforma = dict(
        session.query(Reclamacao.plataforma_nome, func.count(Reclamacao.id))
        .group_by(Reclamacao.plataforma_nome)
        .all()
    )

    por_status = dict(
        session.query(Reclamacao.status, func.count(Reclamacao.id))
        .group_by(Reclamacao.status)
        .all()
    )

    plataformas = session.query(Plataforma).all()
    info_plataformas = [
        {
            "nome":            p.nome,
            "total":           p.total_reclamacoes,
            "ativa":           p.ativa,
            "ultima_raspagem": p.ultima_raspagem.isoformat() if p.ultima_raspagem else None,
        }
        for p in plataformas
    ]

    session.close()

    return {
        "total":          total,
        "recentes_24h":   recentes_24h,
        "por_plataforma": por_plataforma,
        "por_status":     por_status,
        "plataformas":    info_plataformas,
    }


@app.get("/api/tendencia")
async def tendencia(dias: int = Query(30)):
    """Reclamacoes coletadas por dia nos ultimos N dias."""
    session = SessionLocal()
    desde = datetime.now() - timedelta(days=dias)

    rows = (
        session.query(
            func.date(Reclamacao.primeira_coleta).label("dia"),
            func.count(Reclamacao.id).label("total"),
        )
        .filter(Reclamacao.primeira_coleta >= desde)
        .group_by(func.date(Reclamacao.primeira_coleta))
        .order_by("dia")
        .all()
    )
    session.close()

    return {
        "datas":  [str(r.dia) for r in rows],
        "valores": [r.total for r in rows],
    }


# ------------------------------------------------------------------
# CRUD Plataformas
# ------------------------------------------------------------------

class PlataformaCreate(BaseModel):
    nome: str
    url_base: str


@app.get("/api/plataformas")
async def listar_plataformas():
    session = SessionLocal()
    plataformas = session.query(Plataforma).order_by(Plataforma.nome).all()
    session.close()
    return [
        {
            "id":              p.id,
            "nome":            p.nome,
            "url_base":        p.url_base,
            "ativa":           p.ativa,
            "total_reclamacoes": p.total_reclamacoes,
            "ultima_raspagem": p.ultima_raspagem.isoformat() if p.ultima_raspagem else None,
        }
        for p in plataformas
    ]


@app.post("/api/plataformas", status_code=201)
async def adicionar_plataforma(body: PlataformaCreate):
    if "reclameaqui.com.br" not in body.url_base:
        raise HTTPException(400, "URL deve ser do Reclame Aqui")

    nome = body.nome.strip()
    if not nome:
        m = re.search(r"/empresa/([^/]+)", body.url_base)
        nome = m.group(1).replace("-", " ").title() if m else "Nova Plataforma"

    session = SessionLocal()
    if session.query(Plataforma).filter_by(nome=nome).first():
        session.close()
        raise HTTPException(400, f"Plataforma '{nome}' ja existe")

    # Garante que a URL aponta para lista-reclamacoes
    url = body.url_base.rstrip("/")
    if not url.endswith("lista-reclamacoes"):
        url += "/lista-reclamacoes/"
    else:
        url += "/"

    nova = Plataforma(nome=nome, url_base=url, ativa=True)
    session.add(nova)
    session.commit()
    result = {"id": nova.id, "nome": nova.nome}
    session.close()
    return result


@app.put("/api/plataformas/{plataforma_id}")
async def atualizar_plataforma(plataforma_id: int, body: PlataformaCreate):
    session = SessionLocal()
    plat = session.query(Plataforma).filter_by(id=plataforma_id).first()
    if not plat:
        session.close()
        raise HTTPException(404, "Plataforma nao encontrada")
    plat.nome = body.nome.strip()
    plat.url_base = body.url_base
    session.commit()
    session.close()
    return {"mensagem": "Plataforma atualizada"}


@app.delete("/api/plataformas/{plataforma_id}")
async def deletar_plataforma(plataforma_id: int):
    session = SessionLocal()
    plat = session.query(Plataforma).filter_by(id=plataforma_id).first()
    if not plat:
        session.close()
        raise HTTPException(404, "Plataforma nao encontrada")
    plat.ativa = False  # soft delete
    session.commit()
    session.close()
    return {"mensagem": "Plataforma desativada"}


@app.post("/api/plataformas/{plataforma_id}/toggle")
async def toggle_plataforma(plataforma_id: int):
    session = SessionLocal()
    plat = session.query(Plataforma).filter_by(id=plataforma_id).first()
    if not plat:
        session.close()
        raise HTTPException(404, "Plataforma nao encontrada")
    plat.ativa = not plat.ativa
    session.commit()
    ativa = plat.ativa
    session.close()
    return {"ativa": ativa}


# ------------------------------------------------------------------
# Analise: palavras-chave e nichos
# ------------------------------------------------------------------

@app.get("/api/palavras-chave")
async def listar_palavras_chave(limit: int = Query(50, le=200)):
    session = SessionLocal()
    palavras = (
        session.query(PalavraChave)
        .order_by(desc(PalavraChave.frequencia_total))
        .limit(limit)
        .all()
    )
    session.close()
    return [{"palavra": p.palavra, "frequencia": p.frequencia_total} for p in palavras]


@app.get("/api/nichos")
async def listar_nichos():
    session = SessionLocal()
    nichos = (
        session.query(Nicho)
        .order_by(desc(Nicho.total_reclamacoes))
        .all()
    )
    session.close()
    return [
        {
            "nome":         n.nome,
            "total":        n.total_reclamacoes,
            "cor":          n.cor_badge,
            "palavras_chave": json.loads(n.palavras_chave_associadas) if n.palavras_chave_associadas else [],
        }
        for n in nichos
    ]


@app.post("/api/executar-analise")
async def executar_analise(background_tasks: BackgroundTasks):
    from analyzer import processar_analise_completa
    background_tasks.add_task(processar_analise_completa)
    return {"mensagem": "Analise iniciada em background."}


# ------------------------------------------------------------------
# Exportacao CSV
# ------------------------------------------------------------------

@app.get("/api/exportar/csv")
async def exportar_csv(
    plataforma: str = Query(None),
    nicho: str = Query(None),
    status: str = Query(None),
    busca: str = Query(None),
):
    session = SessionLocal()
    query = session.query(Reclamacao)

    if plataforma:
        query = query.filter(Reclamacao.plataforma_nome == plataforma)
    if nicho:
        query = query.filter(Reclamacao.nicho_detectado == nicho)
    if status:
        query = query.filter(Reclamacao.status == status)
    if busca:
        pat = f"%{busca}%"
        query = query.filter(
            (Reclamacao.titulo.ilike(pat)) | (Reclamacao.descricao.ilike(pat))
        )

    rows = query.order_by(desc(Reclamacao.data_reclamacao)).all()
    session.close()

    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Titulo", "Descricao", "Plataforma", "Nicho", "Status", "Data", "URL"])
    for r in rows:
        writer.writerow([
            r.titulo,
            r.descricao or "",
            r.plataforma_nome,
            r.nicho_detectado or "",
            r.status or "",
            r.data_reclamacao.strftime("%d/%m/%Y %H:%M") if r.data_reclamacao else "",
            r.url_original or "",
        ])

    return Response(
        content=output.getvalue().encode("utf-8-sig"),  # BOM para Excel
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=reclamacoes.csv"},
    )


# ------------------------------------------------------------------
# Stopwords
# ------------------------------------------------------------------

class StopwordCreate(BaseModel):
    palavra: str
    categoria: str = "customizada"


@app.get("/stopwords", response_class=HTMLResponse)
async def pagina_stopwords():
    return _html("stopwords.html")


@app.get("/ia", response_class=HTMLResponse)
async def pagina_ia():
    return _html("chat_grok.html")


@app.get("/api/stopwords")
async def listar_stopwords(categoria: str = Query(None)):
    session = SessionLocal()
    query = session.query(Stopword).filter_by(ativa=True)
    if categoria:
        query = query.filter_by(categoria=categoria)
    rows = query.order_by(Stopword.categoria, Stopword.palavra).all()
    session.close()
    return [{"id": s.id, "palavra": s.palavra, "categoria": s.categoria} for s in rows]


@app.post("/api/stopwords", status_code=201)
async def adicionar_stopword(body: StopwordCreate, background_tasks: BackgroundTasks):
    palavra = body.palavra.strip().lower()
    if not palavra:
        raise HTTPException(400, "Palavra invalida")

    session = SessionLocal()
    if session.query(Stopword).filter_by(palavra=palavra).first():
        session.close()
        raise HTTPException(400, f"'{palavra}' ja e uma stopword")

    session.add(Stopword(palavra=palavra, categoria=body.categoria, ativa=True))
    session.commit()
    session.close()

    # Re-processa analise automaticamente
    from analyzer import processar_analise_completa
    background_tasks.add_task(processar_analise_completa)

    return {"mensagem": f"'{palavra}' adicionada. Analise sendo reprocessada."}


@app.delete("/api/stopwords/{stopword_id}")
async def remover_stopword(stopword_id: int, background_tasks: BackgroundTasks):
    session = SessionLocal()
    sw = session.query(Stopword).filter_by(id=stopword_id).first()
    if not sw:
        session.close()
        raise HTTPException(404, "Stopword nao encontrada")
    sw.ativa = False
    session.commit()
    session.close()

    from analyzer import processar_analise_completa
    background_tasks.add_task(processar_analise_completa)
    return {"mensagem": "Stopword removida. Analise sendo reprocessada."}


# ------------------------------------------------------------------
# Grok AI
# ------------------------------------------------------------------

class ChatRequest(BaseModel):
    pergunta: str


@app.post("/api/grok/analisar")
async def executar_analise_grok(
    background_tasks: BackgroundTasks,
    limite: int = Query(100),
    reprocessar: bool = Query(False),
):
    """Dispara análise com Grok em background."""
    from grok_analyzer import processar_analise_grok
    background_tasks.add_task(processar_analise_grok, limite=limite, reprocessar=reprocessar)
    return {"status": "iniciado", "mensagem": f"Analisando até {limite} reclamações com Grok AI."}


@app.post("/api/grok/chat")
async def chat_grok(body: ChatRequest):
    """Chat interativo com Grok usando os produtos detectados como contexto."""
    from grok_client import GrokAI
    session = SessionLocal()
    produtos = (
        session.query(Produto)
        .order_by(desc(Produto.frequencia))
        .limit(50)
        .all()
    )
    session.close()

    contexto = [
        {"nome": p.nome, "nicho": p.nicho_provavel, "frequencia": p.frequencia}
        for p in produtos
    ]

    grok = GrokAI()
    resposta = grok.chat_mineracao(body.pergunta, contexto)
    return {"pergunta": body.pergunta, "resposta": resposta}


@app.get("/api/produtos-nichados")
async def listar_produtos_nichados(
    nicho: str = Query(None),
    min_frequencia: int = Query(1),
    limit: int = Query(50, le=200),
):
    """Lista produtos nichados detectados pelo Grok."""
    session = SessionLocal()
    query = session.query(Produto).filter(Produto.frequencia >= min_frequencia)
    if nicho:
        query = query.filter(Produto.nicho_provavel == nicho)
    produtos = query.order_by(desc(Produto.frequencia)).limit(limit).all()
    session.close()

    return [
        {
            "id": p.id,
            "nome": p.nome,
            "frequencia": p.frequencia,
            "nicho": p.nicho_provavel,
            "plataformas": json.loads(p.plataformas_vendido) if p.plataformas_vendido else [],
            "primeira_deteccao": p.primeira_deteccao.isoformat() if p.primeira_deteccao else None,
            "ultima_deteccao": p.ultima_deteccao.isoformat() if p.ultima_deteccao else None,
        }
        for p in produtos
    ]


@app.get("/api/insights")
async def gerar_insights():
    """Gera insights estratégicos com Grok sobre os produtos detectados."""
    from grok_client import GrokAI
    session = SessionLocal()
    produtos = (
        session.query(Produto)
        .order_by(desc(Produto.frequencia))
        .limit(20)
        .all()
    )
    session.close()

    if not produtos:
        return {"insights": "Nenhum produto analisado ainda. Execute a análise com IA primeiro."}

    lista = [
        {"nome": p.nome, "frequencia": p.frequencia, "nicho": p.nicho_provavel}
        for p in produtos
    ]

    grok = GrokAI()
    insights = grok.gerar_insights(lista)
    return {"insights": insights}


# ------------------------------------------------------------------
# Scraping manual
# ------------------------------------------------------------------

@app.post("/api/executar-scraping")
async def executar_scraping(background_tasks: BackgroundTasks):
    from main_scraper import executar_scraping_completo
    background_tasks.add_task(executar_scraping_completo)
    return {"status": "iniciado", "mensagem": "Scraping rodando em background. Aguarde alguns minutos."}
