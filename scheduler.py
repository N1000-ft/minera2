"""
Agendador APScheduler - executa scraping diariamente as 3h.
"""

from apscheduler.schedulers.background import BackgroundScheduler

_scheduler = None


def iniciar_scheduler():
    global _scheduler
    if _scheduler and _scheduler.running:
        return

    from main_scraper import executar_scraping_completo

    _scheduler = BackgroundScheduler(timezone="America/Sao_Paulo")
    _scheduler.add_job(
        executar_scraping_completo,
        "cron",
        hour=3,
        minute=0,
        id="scraping_diario",
    )
    _scheduler.start()
    print("Scheduler iniciado - scraping diario as 03:00 BRT")
