import subprocess

# Lecture du fichier packages.txt
with open('.app/packages.txt', 'r') as file:
    packages = file.readlines()

# Suppression des espaces et sauts de ligne
packages = [pkg.strip() for pkg in packages]

# Installation des packages
for package in packages:
    subprocess.run(['apt', 'install', package, '-y'])
