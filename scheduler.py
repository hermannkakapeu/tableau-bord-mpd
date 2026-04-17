import schedule
import time
from config import LIENS, INTERVALLE_UNITE, INTERVALLE_VALEUR 
from robot import run_tous_les_liens


def job():
    run_tous_les_liens(LIENS)


# ── Planification ─────────────────────────────────────────────                                           
if INTERVALLE_UNITE == "minutes":
    schedule.every(INTERVALLE_VALEUR).minutes.do(job)
elif INTERVALLE_UNITE == "hours":
    schedule.every(INTERVALLE_VALEUR).hours.do(job)
elif INTERVALLE_UNITE == "days":
    schedule.every(INTERVALLE_VALEUR).days.do(job)
else:
    raise ValueError(f"Unité inconnue : {INTERVALLE_UNITE}")
   
print(f"⏱️  Scheduler démarré — exécution toutes les {INTERVALLE_VALEUR} {INTERVALLE_UNITE}")
print(f"   Prochain lancement : {schedule.next_run()}")   
print("   (Ctrl+C pour arrêter)\n")

# ── Premier lancement immédiat ────────────────────────────────
job()

# ── Boucle principale ─────────────────────────────────────────
while True:
    schedule.run_pending()
    time.sleep(30)


















