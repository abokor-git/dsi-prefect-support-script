#!/bin/bash

# Chemin vers le fichier packages.txt
packages_file="./app/packages.txt"

# Vérification de l'existence du fichier
if [ ! -f "$packages_file" ]; then
  echo "Le fichier $packages_file n'existe pas."
  exit 1
fi

# Lecture du fichier ligne par ligne
while IFS= read -r package; do
  # Installation du package avec la commande appropriée
  # Adapté en fonction de votre distribution Linux
  if command -v apt-get >/dev/null 2>&1; then
    # Ubuntu / Debian
    sudo apt-get install -y "$package"
  elif command -v yum >/dev/null 2>&1; then
    # CentOS / Fedora
    sudo yum install -y "$package"
  elif command -v dnf >/dev/null 2>&1; then
    # Fedora (version récente)
    sudo dnf install -y "$package"
  else
    echo "La commande d'installation de paquet n'est pas prise en charge sur ce système."
    exit 1
  fi
done < "$packages_file"
