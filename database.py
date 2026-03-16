import os
import hashlib
from datetime import datetime

from dotenv import load_dotenv
from sqlalchemy import (
    Boolean, Column, DateTime, Integer, String, Text,
    UniqueConstraint, create_engine, text,
)
from sqlalchemy.orm import DeclarativeBase, sessionmaker

load_dotenv()

DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///reclamacoes.db')
_is_sqlite = DATABASE_URL.startswith('sqlite')
engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if _is_sqlite else {},
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Base(DeclarativeBase):
    pass


class Plataforma(Base):
    __tablename__ = 'plataformas'

    id = Column(Integer, primary_key=True)
    nome = Column(String(100), unique=True, nullable=False)
    url_base = Column(String(500), nullable=False)
    ativa = Column(Boolean, default=True)
    ultima_raspagem = Column(DateTime)
    total_reclamacoes = Column(Integer, default=0)


class Reclamacao(Base):
    __tablename__ = 'reclamacoes'

    id = Column(Integer, primary_key=True)
    hash_id = Column(String(64), unique=True, nullable=False)
    plataforma_nome = Column(String(100), nullable=False)

    titulo = Column(String(500), nullable=False)
    descricao = Column(Text)
    data_reclamacao = Column(DateTime)
    status = Column(String(50))
    aba_origem = Column(String(50))
    url_original = Column(String(500))
    nicho_detectado = Column(String(100))
    produto_mencionado = Column(String(200))
    palavras_chave_extraidas = Column(Text)  # JSON com análise completa do Grok

    primeira_coleta = Column(DateTime, default=datetime.now)
    ultima_atualizacao = Column(DateTime, default=datetime.now, onupdate=datetime.now)

    __table_args__ = (UniqueConstraint('hash_id', name='uix_hash_id'),)


class PalavraChave(Base):
    __tablename__ = 'palavras_chave'

    id = Column(Integer, primary_key=True)
    palavra = Column(String(100), unique=True, nullable=False)
    frequencia_total = Column(Integer, default=0)
    primeira_aparicao = Column(DateTime, default=datetime.now)
    ultima_aparicao = Column(DateTime, default=datetime.now)


class Nicho(Base):
    __tablename__ = 'nichos'

    id = Column(Integer, primary_key=True)
    nome = Column(String(100), unique=True, nullable=False)
    total_reclamacoes = Column(Integer, default=0)
    palavras_chave_associadas = Column(Text)  # JSON
    cor_badge = Column(String(20), default='blue')


class Stopword(Base):
    __tablename__ = 'stopwords'

    id = Column(Integer, primary_key=True)
    palavra = Column(String(100), unique=True, nullable=False)
    categoria = Column(String(50), default='customizada')  # customizada | generico | juridico | tecnico | sentimento | faturamento
    ativa = Column(Boolean, default=True)
    criado_em = Column(DateTime, default=datetime.now)


class Produto(Base):
    __tablename__ = 'produtos'

    id = Column(Integer, primary_key=True)
    nome = Column(String(200), unique=True, nullable=False)
    frequencia = Column(Integer, default=1)
    nicho_provavel = Column(String(100))
    plataformas_vendido = Column(Text)  # JSON list
    primeira_deteccao = Column(DateTime, default=datetime.now)
    ultima_deteccao = Column(DateTime, default=datetime.now)


def gerar_hash_reclamacao(ra_id: str, plataforma: str) -> str:
    unique = f"{ra_id}:{plataforma}".lower().strip()
    return hashlib.sha256(unique.encode()).hexdigest()


PLATAFORMAS_INICIAIS = [
    {'nome': 'PerfectPay',  'url_base': 'https://www.reclameaqui.com.br/empresa/perfectpay/lista-reclamacoes/'},
    {'nome': 'Kiwify',      'url_base': 'https://www.reclameaqui.com.br/empresa/kiwify/lista-reclamacoes/'},
    {'nome': 'CartX',       'url_base': 'https://www.reclameaqui.com.br/empresa/cartx/lista-reclamacoes/'},
    {'nome': 'Hotmart',     'url_base': 'https://www.reclameaqui.com.br/empresa/hotmart/lista-reclamacoes/'},
    {'nome': 'Wiapy',       'url_base': 'https://www.reclameaqui.com.br/empresa/wiapy/lista-reclamacoes/'},
    {'nome': 'SoutPay',     'url_base': 'https://www.reclameaqui.com.br/empresa/soutpay-financeira/lista-reclamacoes/'},
    {'nome': 'Paradise',    'url_base': 'https://www.reclameaqui.com.br/empresa/paradise-tecnologia-servicos-e-pagamentos-ltda/lista-reclamacoes/'},
    {'nome': 'Disrupty',    'url_base': 'https://www.reclameaqui.com.br/empresa/disrupty-tecnologia-servicos-e-pagamentos-ltda/lista-reclamacoes/'},
    {'nome': 'LastLink',    'url_base': 'https://www.reclameaqui.com.br/empresa/lastlink/lista-reclamacoes/'},
    {'nome': 'Greenn',      'url_base': 'https://www.reclameaqui.com.br/empresa/greenn/lista-reclamacoes/'},
]


def init_db():
    Base.metadata.create_all(engine)

    # Migracao segura: adiciona coluna nicho_detectado se nao existir
    with engine.connect() as conn:
        migrações = [
            "ALTER TABLE reclamacoes ADD COLUMN nicho_detectado VARCHAR(100)",
            "ALTER TABLE reclamacoes ADD COLUMN produto_mencionado VARCHAR(200)",
            "ALTER TABLE reclamacoes ADD COLUMN palavras_chave_extraidas TEXT",
        ]
        for sql in migrações:
            try:
                conn.execute(text(sql))
                conn.commit()
            except Exception:
                pass  # coluna ja existe

    session = SessionLocal()
    for p in PLATAFORMAS_INICIAIS:
        if not session.query(Plataforma).filter_by(nome=p['nome']).first():
            session.add(Plataforma(**p))
    session.commit()
    session.close()
    print("DB inicializado.")
