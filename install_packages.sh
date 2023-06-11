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
  apt install "$package" -y
  
done < "$packages_file"
