url = "https://dadosabertos.ans.gov.br/FTP/PDA/"

import requests
r = requests.get(url)
print(r.status_code)

print(r.text)