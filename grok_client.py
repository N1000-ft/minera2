"""
Cliente da API Grok (xAI) para análise inteligente de reclamações.
"""

import json
import os
import re
from typing import Dict, List

import requests
from dotenv import load_dotenv

load_dotenv()


class GrokAI:
    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("GROK_API_KEY")
        self.base_url = "https://api.x.ai/v1"
        self.model = "grok-3-latest"

    def chat(self, messages: List[Dict], temperature: float = 0.7) -> str | None:
        """Envia mensagens para a API do Grok e retorna a resposta."""
        try:
            response = requests.post(
                f"{self.base_url}/chat/completions",
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                },
                json={
                    "model": self.model,
                    "messages": messages,
                    "temperature": temperature,
                    "stream": False,
                },
                timeout=45,
            )
            response.raise_for_status()
            return response.json()["choices"][0]["message"]["content"]
        except Exception as e:
            print(f"[Grok] Erro na API: {e}")
            return None

    def analisar_reclamacao(self, titulo: str, descricao: str) -> Dict | None:
        """
        Analisa uma reclamação e extrai produto nichado, nicho, score e palavras-chave criativas.
        """
        prompt = (
            "Você é um especialista em marketing de afiliados brasileiro. "
            "Analise esta reclamação do Reclame Aqui e extraia informações estruturadas.\n\n"
            f"TÍTULO: {titulo}\n"
            f"DESCRIÇÃO: {descricao}\n\n"
            "RETORNE APENAS UM JSON (sem markdown) com esta estrutura EXATA:\n"
            '{"produto_mencionado": "nome exato do produto ou null", '
            '"e_produto_nichado": true/false, '
            '"nicho": "emagrecimento|relacionamento|renda_extra|saude|beleza|esoterico|desenvolvimento_pessoal|financas|educacao|esporte|outro|desconhecido", '
            '"palavras_chave_criativas": ["palavra1", "palavra2"], '
            '"score_oportunidade": 0, '
            '"motivo_score": "explicação breve"}\n\n'
            "CRITÉRIOS:\n"
            "- Produto nichado = nome criativo/inusitado que indica oferta específica\n"
            "- Exemplos NICHADOS: 'Mounjaro de Pobre', 'Método da Vovó', 'Desenho Alma Gêmea'\n"
            "- Exemplos NÃO NICHADOS: 'Curso de Excel', 'Consultoria Genérica'\n"
            "- Score 8-10: nome muito criativo + nicho definido\n"
            "- Score 0-3: genérico ou sem produto identificável"
        )

        messages = [
            {
                "role": "system",
                "content": "Você é um analisador de ofertas digitais. Retorne APENAS JSON válido, sem formatação markdown.",
            },
            {"role": "user", "content": prompt},
        ]

        resposta = self.chat(messages, temperature=0.3)
        if not resposta:
            return None

        try:
            # Remove markdown se houver
            texto = resposta.strip()
            if texto.startswith("```"):
                partes = texto.split("```")
                texto = partes[1]
                if texto.startswith("json"):
                    texto = texto[4:]

            # Extrai JSON mesmo que haja texto ao redor
            match = re.search(r"\{.*\}", texto, re.DOTALL)
            if match:
                texto = match.group()

            return json.loads(texto)
        except Exception as e:
            print(f"[Grok] Erro ao parsear JSON: {e}")
            return None

    def chat_mineracao(self, pergunta: str, contexto_produtos: List[Dict]) -> str:
        """
        Chat interativo sobre os produtos e nichos detectados.
        """
        resumo = "\n".join([
            f"- {p['nome']} (nicho: {p['nicho']}, frequência: {p['frequencia']}x)"
            for p in contexto_produtos[:30]
            if p.get("nome")
        ]) or "Nenhum produto detectado ainda."

        prompt = (
            "Você é um assistente especializado em mineração de ofertas para afiliados brasileiros.\n\n"
            f"PRODUTOS DETECTADOS NAS RECLAMAÇÕES:\n{resumo}\n\n"
            f"PERGUNTA: {pergunta}\n\n"
            "Responda de forma direta e prática, focando em oportunidades de afiliação."
        )

        messages = [
            {
                "role": "system",
                "content": "Você é um especialista em marketing de afiliados e mineração de ofertas digitais no Brasil.",
            },
            {"role": "user", "content": prompt},
        ]

        return self.chat(messages, temperature=0.7) or "Não foi possível obter resposta da IA."

    def gerar_insights(self, produtos: List[Dict]) -> str:
        """Gera insights estratégicos sobre os produtos detectados."""
        lista = "\n".join([
            f"- {p['nome']} ({p['frequencia']}x no nicho {p['nicho']})"
            for p in produtos
        ]) or "Nenhum produto ainda."

        prompt = (
            f"Analise estes produtos detectados em reclamações e gere insights para afiliados:\n\n"
            f"{lista}\n\n"
            "Retorne:\n"
            "1. Top 3 nichos mais quentes agora\n"
            "2. Padrões de naming (que tipo de nome vende?)\n"
            "3. Oportunidades subexploradas\n"
            "4. Red flags (produtos a evitar)\n\n"
            "Seja direto e prático."
        )

        return self.chat([{"role": "user", "content": prompt}]) or "Sem insights disponíveis."
