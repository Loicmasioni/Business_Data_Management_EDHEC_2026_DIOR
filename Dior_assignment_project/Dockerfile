# 1. Utiliser une image Python légère comme base
FROM python:3.11-slim

# 2. Installer les outils système nécessaires (certificats et curl)
RUN apt-get update && apt-get install -y \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# 3. Définir le dossier de travail à l'intérieur du container
WORKDIR /app

# 4. Copier le fichier des dépendances et les installer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 5. Copier tout le reste du projet dans le container
COPY . .

# 6. Exposer le port pour JupyterLab
EXPOSE 8888

# 7. Lancer JupyterLab quand le container démarre
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]