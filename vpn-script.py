import os
import subprocess

server = os.getenv('SERVER')
server_cert = os.getenv('SERVER_CERT')
user = os.getenv('USER')
password = os.getenv('PASSWORD')

# Construire la commande à exécuter
command = f"openconnect {server} --servercert pin-sha256:{server_cert} --user={user} --key-password={password} <<EOF\n{password}\nEOF &"

# Exécuter la commande en arrière-plan
subprocess.Popen(command, shell=True, stdout=subprocess.DEVNULL)
