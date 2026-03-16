"""
Analisador de palavras-chave e nichos das reclamacoes.
"""

import json
import os
import re
import requests
from collections import Counter
from datetime import datetime

from database import SessionLocal, Reclamacao, PalavraChave, Nicho, Stopword

# ------------------------------------------------------------------
# Stopwords PT-BR expandidas
# ------------------------------------------------------------------
STOPWORDS = {
    # ── Artigos / preposicoes / conjuncoes ──────────────────────────
    "de", "a", "o", "que", "e", "do", "da", "em", "um", "para", "com",
    "uma", "os", "no", "se", "na", "por", "mais", "as", "dos", "como",
    "mas", "ao", "ele", "das", "tem", "seu", "sua", "ou", "ser", "ela",
    "nos", "ja", "me", "meu", "minha", "foi", "esse", "essa", "isso",
    "este", "esta", "pelo", "pela", "pois", "pra", "pro", "nem", "ate",
    "sob", "sem", "sobre", "entre", "apos", "ante", "desde",
    # ── Verbos comuns ────────────────────────────────────────────────
    "fiz", "fez", "fui", "vai", "vou", "pode", "fazer", "ter", "estar",
    "sendo", "tendo", "feito", "ficou", "fica", "tive", "tenho", "tinha",
    "quero", "queria", "comprei", "comprou", "recebi", "recebu",
    "entrei", "entrou", "tentei", "tentou", "enviei", "enviou",
    # ── Reclamacao generica ──────────────────────────────────────────
    "reembolso", "cancelamento", "cancelar", "atendimento", "suporte",
    "senha", "login", "acesso", "site", "plataforma", "empresa",
    "problema", "problemas", "reclamacao", "reclamacoes", "reclamar",
    "servico", "servicos", "produto", "produtos", "compra", "compras",
    "valor", "valores", "pagar", "pagamento", "pagamentos", "cobrado",
    "cobranca", "cobrancas", "conta", "contas", "dinheiro", "retorno",
    "resposta", "contato", "email", "telefone", "whatsapp", "dias",
    "meses", "horas", "semanas", "vezes", "vez", "nenhum", "nenhuma",
    "ainda", "depois", "antes", "agora", "aqui", "nao", "sim", "nunca",
    "sempre", "muito", "pouco", "mais", "menos", "tudo", "nada",
    "todo", "toda", "todos", "todas", "outro", "outra", "outros",
    "estou", "estava", "eles", "voce", "voces", "cliente", "clientes",
    "mesmo", "mesma", "apenas", "somente", "tambem", "entao",
    "porque", "quando", "onde", "como", "qual", "quais",
    # ── Plataformas (nome proprio) ───────────────────────────────────
    "kiwify", "hotmart", "perfectpay", "cartx", "wiapy", "soutpay",
    "paradise", "disrupty", "lastlink", "greenn", "eduzz", "monetizze",
    # ── Juridicas ───────────────────────────────────────────────────
    "processar", "processando", "processei", "judicializar", "intimar",
    "denunciar", "denunciando", "denunciei", "delatar", "processual",
    "litigioso", "peticao", "liminar", "audiencia", "forum", "juiz",
    "justica", "advogado", "defensoria", "ministerio", "promotor",
    "estelionato", "crime", "criminal", "policia", "delegacia",
    "boletim", "ocorrencia", "penal", "civil", "defesa", "consumidor",
    "procon", "tribunal", "julgamento", "condenacao", "multa",
    "indenizacao", "danos", "morais", "calunia", "difamacao",
    # ── Sentimentos ──────────────────────────────────────────────────
    "odiei", "pessimo", "ruim", "horrivel", "terrivel", "bosta", "lixo",
    "porcaria", "enganado", "enganada", "tapeado", "tapeada",
    "ludibriado", "ludibriada", "roubo", "furto", "saque", "prejuizo",
    "perdi", "arrependido", "arrependida", "frustrado", "frustrada",
    "mentira", "farsa", "fake", "fajuto", "vagabundo", "amador",
    "desorganizado",
    # ── Tecnicas ─────────────────────────────────────────────────────
    "esqueci", "trocar", "redefinir", "area", "membros", "falhou",
    "consigo", "entrar", "confirmacao", "aluno", "dashboard", "fora",
    "travado", "lento", "carregando", "baixar", "offline", "sincronizar",
    "atualizacao", "versao", "antiga", "aplicativo", "abre",
    # ── Faturamento ──────────────────────────────────────────────────
    "nota", "fiscal", "recibo", "comprovante", "boleto", "vencido",
    "segunda", "via", "faturamento", "extrato", "fatura", "indevida",
    "duplicidade", "cartao", "recusado", "estornar", "cade",
    "atraso", "demora", "aguardando", "espera",
    # ── Pirataria / gratuito ─────────────────────────────────────────
    "free", "download", "direto", "mega", "mediafire", "google", "drive",
    "telegram", "grupo", "vip", "gratis", "graca", "faixa", "cortesia",
    "brinde", "amostra", "trial", "teste", "demo", "demonstrativo",
    "crack", "crackeado", "ativador", "serial", "chave", "ativacao",
    "torrent", "pirata", "completo", "hack", "apk", "premium",
    "desbloqueado", "bypass", "burlar", "gerador",
    # ── Customizadas pelo usuario ────────────────────────────────────
    "curso", "solicitacao", "editado", "reclame", "falta", "estorno",
    "instagram", "propaganda", "engano", "sara", "realizei",
    "dificuldade", "dentro", "prazo", "conteudo", "volta", "taxas",
    "gostaria", "solicito", "paguei", "porem", "saldo", "devido",
    "link", "tiktok", "impossibilidade", "gostei", "venho", "efetuado",
    "tarde", "momento", "solicitar", "atraves", "enganosa", "mail",
    "taxa", "acessar", "agencia", "banco", "numero", "codigo",
}

# ------------------------------------------------------------------
# Nichos com palavras-chave associadas
# ------------------------------------------------------------------
NICHOS = {
    "Emagrecimento": [
        "emagrecer", "emagrecimento", "dieta", "peso", "barriga", "gordura",
        "queimar", "magro", "magra", "obesidade", "calorias", "metabolismo",
        "jejum", "detox", "termogenico", "slim", "fitness", "academia",
    ],
    "Relacionamento": [
        "amor", "relacionamento", "namorado", "namorada", "casamento", "ex",
        "conquista", "seducao", "paquera", "matrimonio", "divorcio", "reconciliar",
        "separacao", "infidelidade", "traicao", "ciumes",
    ],
    "Renda Extra": [
        "renda", "ganhar", "dinheiro", "afiliado", "afiliados", "comissao",
        "vender", "vendas", "negocio", "empreendedor", "trabalhar", "home",
        "online", "dropshipping", "estoque", "lucro", "faturamento",
        "monetizar", "marketing", "digital",
    ],
    "Saude": [
        "saude", "dor", "tratamento", "remedio", "doenca", "cura", "terapia",
        "medico", "medicamento", "suplemento", "vitamina", "colesterol",
        "diabetes", "pressao", "tireoide", "articulacao", "imunidade",
    ],
    "Beleza": [
        "cabelo", "pele", "rosto", "rugas", "estetica", "beleza", "aparencia",
        "maquiagem", "creme", "serum", "antiidade", "cilios", "sobrancelha",
        "depilacao", "bronzeamento",
    ],
    "Desenvolvimento Pessoal": [
        "autoestima", "confianca", "ansiedade", "depressao", "mindset",
        "produtividade", "habitos", "meditacao", "motivacao", "lideranca",
        "comportamento", "emocional", "inteligencia",
    ],
    "Esoterico": [
        "feitico", "simpatia", "energia", "oracao", "protecao", "amarracao",
        "banho", "velas", "magia", "espiritualidade", "tarô", "tarot",
        "horoscopo", "universo", "manifestacao",
    ],
    "Financas": [
        "investimento", "acoes", "bolsa", "cripto", "bitcoin", "renda",
        "aposentadoria", "poupanca", "tesouro", "dividendos", "fintech",
        "credito", "emprestimo", "financiamento",
    ],
    "Educacao": [
        "curso", "cursos", "aula", "aulas", "treinamento", "certificado",
        "diploma", "concurso", "idioma", "ingles", "espanhol", "programacao",
        "codigo", "faculdade", "vestibular",
    ],
    "Esporte": [
        "treino", "musculacao", "crossfit", "corrida", "futebol", "volei",
        "natacao", "ciclismo", "proteina", "whey", "suplementacao",
    ],
}

CORES_NICHOS = [
    "blue", "green", "purple", "pink", "yellow", "red", "indigo",
    "orange", "teal", "cyan",
]


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------

def _normalizar(texto: str) -> str:
    """Remove acentos e normaliza para lowercase."""
    import unicodedata
    nfkd = unicodedata.normalize("NFKD", texto)
    sem_acento = "".join(c for c in nfkd if not unicodedata.combining(c))
    return sem_acento.lower()


def _carregar_stopwords_db() -> set:
    """Carrega stopwords adicionais cadastradas no banco."""
    try:
        session = SessionLocal()
        extras = {s.palavra for s in session.query(Stopword).filter_by(ativa=True).all()}
        session.close()
        return extras
    except Exception:
        return set()


def extrair_palavras(texto: str, stopwords_extras: set = None) -> list:
    """Retorna lista de tokens relevantes de um texto."""
    if not texto:
        return []
    texto = _normalizar(texto)
    texto = re.sub(r"[^\w\s]", " ", texto)
    tokens = texto.split()
    bloqueadas = STOPWORDS | (stopwords_extras or set())
    return [
        t for t in tokens
        if len(t) >= 4
        and not t.isdigit()
        and t not in bloqueadas
    ]


def detectar_nicho(texto: str) -> str | None:
    """Detecta o nicho mais provavel para um texto."""
    if not texto:
        return None
    texto_norm = _normalizar(texto)
    scores = {}
    for nicho, palavras in NICHOS.items():
        score = sum(1 for p in palavras if p in texto_norm)
        if score > 0:
            scores[nicho] = score
    return max(scores, key=scores.get) if scores else None


def extrair_produto(texto: str) -> str | None:
    """Tenta extrair nome de produto mencionado."""
    if not texto:
        return None
    patterns = [
        r'curso[s]? ["\']?([A-Z][^\.,!?\n]{3,40})',
        r'produto[s]? ["\']?([A-Z][^\.,!?\n]{3,40})',
        r'comprei [oa]? ?["\']?([A-Z][^\.,!?\n]{3,40})',
        r'programa[s]? ["\']?([A-Z][^\.,!?\n]{3,40})',
    ]
    for pattern in patterns:
        m = re.search(pattern, texto)
        if m:
            return m.group(1).strip()[:60]
    return None


# ------------------------------------------------------------------
# Filtro com Grok (xAI)
# ------------------------------------------------------------------

def filtrar_palavras_com_grok(palavras_counter: Counter, top_n: int = 300) -> Counter:
    """
    Envia as top N palavras para o Grok e pede para manter apenas
    as que revelam nichos, produtos ou temas específicos de infoprodutos.
    Retorna um Counter filtrado.
    """
    api_key = os.getenv("GROK_API_KEY")
    if not api_key:
        print("[Grok] GROK_API_KEY não encontrada, pulando filtro.")
        return palavras_counter

    top_palavras = [p for p, _ in palavras_counter.most_common(top_n)]
    if not top_palavras:
        return palavras_counter

    print(f"[Grok] Filtrando {len(top_palavras)} palavras via Grok...")

    prompt = (
        "Você é um especialista em marketing de infoprodutos digitais brasileiros.\n"
        "Abaixo estão palavras extraídas de reclamações de clientes em plataformas como Hotmart e Kiwify.\n\n"
        "Retorne APENAS as palavras que indicam:\n"
        "- Nichos de infoprodutos (ex: emagrecimento, relacionamento, investimento, fitness)\n"
        "- Produtos ou cursos específicos mencionados\n"
        "- Temas que revelam o que o cliente comprou ou o problema de nicho\n\n"
        "REMOVA palavras genéricas de reclamação (cancelar, reembolso, atendimento, problema, acesso, login, senha, etc).\n\n"
        'Responda SOMENTE com JSON válido no formato: {"palavras_relevantes": ["palavra1", "palavra2", ...]}\n\n'
        f"Palavras para analisar:\n{', '.join(top_palavras)}"
    )

    try:
        resp = requests.post(
            "https://api.x.ai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
            json={
                "model": "grok-3-latest",
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.1,
            },
            timeout=45,
        )
        resp.raise_for_status()

        conteudo = resp.json()["choices"][0]["message"]["content"]

        # Extrai o JSON da resposta (ignora texto fora do JSON)
        match = re.search(r'\{.*\}', conteudo, re.DOTALL)
        if not match:
            raise ValueError("JSON não encontrado na resposta do Grok")

        dados = json.loads(match.group())
        relevantes = set(dados.get("palavras_relevantes", []))

        # Retorna Counter filtrado mantendo apenas as palavras aprovadas pelo Grok
        filtrado = Counter({p: c for p, c in palavras_counter.items() if p in relevantes})

        print(f"[Grok] {len(top_palavras)} → {len(filtrado)} palavras relevantes")
        return filtrado

    except Exception as e:
        print(f"[Grok] Erro: {e} — usando palavras sem filtro Grok")
        return palavras_counter


# ------------------------------------------------------------------
# Analise principal
# ------------------------------------------------------------------

def processar_analise_completa():
    """Processa todas as reclamacoes: detecta nichos e extrai palavras-chave."""
    session = SessionLocal()
    print("Iniciando analise de palavras-chave e nichos...")

    reclamacoes = session.query(Reclamacao).all()
    if not reclamacoes:
        print("Nenhuma reclamacao para analisar.")
        session.close()
        return

    # Carrega stopwords extras do banco (cadastradas pelo usuario)
    stopwords_extras = _carregar_stopwords_db()

    # --- Detecta nicho por reclamacao ---
    contador_palavras = Counter()
    contador_nichos = Counter()

    for rec in reclamacoes:
        texto = f"{rec.titulo} {rec.descricao or ''}"
        tokens = extrair_palavras(texto, stopwords_extras)
        contador_palavras.update(tokens)

        nicho = detectar_nicho(texto)
        if nicho:
            rec.nicho_detectado = nicho
            contador_nichos[nicho] += 1

    session.commit()

    # --- Filtra palavras com Grok ---
    contador_palavras = filtrar_palavras_com_grok(contador_palavras)

    # --- Atualiza tabela palavras_chave ---
    session.query(PalavraChave).delete()
    for palavra, freq in contador_palavras.most_common(300):
        session.add(PalavraChave(
            palavra=palavra,
            frequencia_total=freq,
            ultima_aparicao=datetime.now(),
        ))
    session.commit()

    # --- Atualiza tabela nichos ---
    session.query(Nicho).delete()
    for i, (nicho, total) in enumerate(contador_nichos.most_common()):
        session.add(Nicho(
            nome=nicho,
            total_reclamacoes=total,
            cor_badge=CORES_NICHOS[i % len(CORES_NICHOS)],
            palavras_chave_associadas=json.dumps(NICHOS.get(nicho, []), ensure_ascii=False),
        ))
    session.commit()
    session.close()

    print(f"Analise concluida: {len(contador_palavras)} palavras | {len(contador_nichos)} nichos detectados")
