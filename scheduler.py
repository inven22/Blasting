# scheduler.py

from apscheduler.schedulers.background import BackgroundScheduler

# Inisialisasi scheduler tunggal yang bisa digunakan seluruh project
scheduler = BackgroundScheduler()
scheduler.start()
