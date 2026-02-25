import requests

TOKEN = "8456900464:AAEn1Qnhz2ONsVb97OmgSh-uP7DP5_WsZGo"

url = f"https://api.telegram.org/bot{TOKEN}/getUpdates"
r = requests.get(url)
print(r.json())