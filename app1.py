import streamlit as st
import ollama
from gtts import gTTS
import re  # Utilisé pour nettoyer les balises Markdown

# Fonction pour nettoyer le texte des balises Markdown
def clean_markdown(text):
    """Supprime les balises Markdown (comme **) du texte."""
    return re.sub(r"\*\*(.*?)\*\*", r"\1", text)

# Fonction TTS adaptée
def text_to_speech(text, emotion="neutre"):
    """Convertit le texte en audio avec une tonalité adaptée à l'émotion."""
    tts = gTTS(text, lang="fr")
    audio_file = f"response_{emotion}.mp3"
    tts.save(audio_file)
    return audio_file

# Détection des émotions
def detect_emotion_from_message(message):
    """Détecte les émotions basées sur des mots-clés en contexte RH."""
    if any(word in message.lower() for word in ["stress", "nerveux", "anxieux", "doute"]):
        return "stress"
    elif any(word in message.lower() for word in ["confiant", "positif", "prêt"]):
        return "confiance"
    elif any(word in message.lower() for word in ["perdu", "confus", "difficile"]):
        return "confusion"
    else:
        return "neutre"

# Génération de réponses
def generate_response_with_emotion(model, message, emotion, config):
    """Génère une réponse personnalisée en fonction de l'émotion détectée."""
    base_prompt = "Vous êtes un consultant RH expérimenté."
    if emotion == "stress":
        base_prompt += " Aidez le candidat à se détendre avec des conseils rassurants."
    elif emotion == "confiance":
        base_prompt += " Félicitez et conseillez le candidat pour perfectionner sa performance."
    elif emotion == "confusion":
        base_prompt += " Clarifiez les doutes avec des explications simples et détaillées."
    else:
        base_prompt += " Fournissez des conseils RH pratiques et objectifs."
    
    messages = [
        {"role": "system", "content": base_prompt},
        {"role": "user", "content": message}
    ]
    
    full_response = ""
    for response in ollama.chat(model=model, messages=messages, options=config, stream=True):
        full_response += response['message']['content']
    return full_response

# Interface utilisateur
st.title("Consultant RH Virtuel avec Synthèse Vocale")

# Sélection du modèle
st.sidebar.header("Options de configuration")
available_models = ["llama3.2", "qwen:0.5b"]
selected_model = st.sidebar.selectbox("Modèle", available_models, index=0)
temperature = st.sidebar.slider("Température", 0.0, 2.0, 0.7, 0.1)
top_k = st.sidebar.slider("Top-k", 1, 100, 40, 1)
top_p = st.sidebar.slider("Top-p", 0.0, 1.0, 0.9, 0.05)
max_tokens = st.sidebar.slider("Nombre maximal de tokens", 1, 2048, 512, 1)

# Entrée utilisateur
user_input = st.text_input("Posez votre question ou décrivez une situation :")
if user_input:
    # Détection de l'émotion
    emotion = detect_emotion_from_message(user_input)

    # Génération de la réponse
    response = generate_response_with_emotion(
        selected_model,
        user_input,
        emotion,
        {"temperature": temperature, "top_k": top_k, "top_p": top_p, "num_predict": max_tokens}
    )

    # Nettoyer la réponse pour la synthèse vocale
    clean_response = clean_markdown(response)

    # Affichage dans l'interface
    st.write(f"Emotion détectée : **{emotion}**")
    st.write(f"Réponse : {response}")  # Affichage avec Markdown

    # Synthèse vocale avec le texte nettoyé
    audio_file = text_to_speech(clean_response, emotion)
    st.audio(audio_file)
